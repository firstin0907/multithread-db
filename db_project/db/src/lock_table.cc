#include "../include/lock_table.h"

#include <pthread.h>
#include <cstdio>

#include <vector>

#include "../include/trx.h"

struct lock_list_t
{
  int64_t   table_id;
  pagenum_t page_id;

  lock_t* head;
  lock_t* tail;

  lock_list_t(int64_t table_id, pagenum_t page_id)
  {
    this->table_id = table_id;
    this->page_id = page_id;
    this->head = this->tail = nullptr;
  };
};

template<uint32_t INIT_SIZE = 7>
struct lock_table_t
{
  uint64_t size;
  std::vector<lock_list_t*> table;

  lock_table_t() : table(INIT_SIZE, nullptr), size(INIT_SIZE)
  {
    
  }

  static size_t hash_f(uint64_t h_param, int64_t x, uint64_t y)
  {
    uint64_t trans_x = x, trans_y = y;
    x %= h_param, y %= h_param;

    uint64_t trans_offset =
      (((1LL << 32) % h_param) * ((1LL << 32) % h_param)) % h_param;
    
    trans_x = (trans_x * trans_offset) % h_param;

    return (trans_x + trans_y) % h_param;
  }

  void extend()
  {
    
    size_t next_size = size * INIT_SIZE;
    // extend size of table
    table.resize(next_size);

    for(size_t i = size; i < next_size; i++) table[i] = nullptr;

    for(size_t i = 0; i < size; i++)
    {
      // if there was a list in this entry
      if(table[i] != nullptr)
      {
        size_t entry_num = hash_f(next_size, table[i]->table_id, table[i]->page_id);
        // this list should change the entry
        if(entry_num != i)
        {
          // change place
          table[entry_num] = table[i];
          
          // set originally used entry as nullptr
          table[i] = nullptr; 
        }
      }
    }
    size = next_size;
  }

  lock_list_t* get_list(int64_t table_id, pagenum_t page_id)
  {
    while(1)
    {
      size_t index = hash_f(size, table_id, page_id);

      // if this entry is emtpy,
      if(table[index] == nullptr)
      {
        // then allocate this entry for this <table_id, page_id> pair
        table[index] = new lock_list_t(table_id, page_id);
        return table[index];
      }

      // if this entry is already allocated for this <table_id, page_id> pair
      if(table[index]->table_id == table_id && table[index]->page_id == page_id)
      {
        return table[index];
      }
      
      // if this entry is already allocated for other <table_id, page_id> pair
      else
      {
        // then entend table (costly)
        extend();
      }
    }
  }

  ~lock_table_t()
  {
    for(auto &i : table)
    {
      if(i != nullptr) delete i;
    }
  }

};

lock_table_t<7> Lock_table;

typedef struct lock_t lock_t;

pthread_mutex_t lock_table_latch = PTHREAD_MUTEX_INITIALIZER;

int init_lock_table()
{
  return 0;
}

lock_t* lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key,
    int trx_id, int lock_mode)
{
  // create new lock_t object
  lock_t* lock_object = new lock_t(lock_mode, table_id, page_id, key);

  pthread_mutex_lock(&trx_table_latch);

  // pointer refers to trx instance for this trx_id
  Transaction* curr_trx = trx_manager.trx_table[trx_id];

  for(auto it = curr_trx->lock_ptr; it != nullptr; it = it->trx_next)
  {
    // check this trx already has requested lock
    if(it->table_id == table_id && it->page_id == page_id && it->key == key)
    {
      if(it->lock_mode == LOCK_MODE_EXCLUSIVE || lock_mode == LOCK_MODE_SHARED)
      {
        // if this is not acquired yet, wait until it is acquired.
        if(it->is_acquired == false)
        {
          it->successor_cnt++;
          pthread_cond_wait(&it->acq_cond, &trx_table_latch);
          it->successor_cnt--;
        }
          
        if(it->is_acquired == false)
        {
          if(it->successor_cnt <= 0) pthread_cond_broadcast(&(it->delete_cond));
          
          delete lock_object;
          pthread_mutex_unlock(&trx_table_latch);
          throw DeadlockDetectException();
        }
        else
        {
          if(it->successor_cnt <= 0) pthread_cond_broadcast(&(it->delete_cond));
          
          delete lock_object;
          pthread_mutex_unlock(&trx_table_latch);
          return it;
        }
      }
    }
  }

  // if there are not matching lock
  // insert it into trx lock list
  lock_object->trx_next = curr_trx->lock_ptr;
  curr_trx->lock_ptr = lock_object;

  // set remained attribute
  lock_object->owner_trx = curr_trx;
  lock_object->owner_trx_id = trx_id;
  pthread_mutex_unlock(&trx_table_latch);
  

  pthread_mutex_lock(&lock_table_latch);
  try
  {
    // get the list for this lock_t object to be inserted.
    auto lock_list = Lock_table.get_list(table_id, page_id);

    // initialize some fields of new lock_t object to be inserted.
    lock_object->prev_pointer = lock_list->tail;
    lock_object->next_pointer = nullptr;
    // set tail as this object
    lock_list->tail = lock_object;


    // if this is no predecessor’s lock object,
    if(lock_object->prev_pointer == nullptr)
    {
      // then set list's head to created object
      lock_list->head = lock_object;
    }
    else
    {
      // set previous element's pointer 
      lock_object->prev_pointer->next_pointer = lock_object;

      int flag = 0;
      lock_t* it = lock_object->prev_pointer;
      while(it != nullptr)
      {
        // if it is end, or it lock other key, or is holded by same trx,
        // we can ignore it.
        if(it->is_end == true || it->key != key || it->owner_trx_id == trx_id)
        {
          it = it->prev_pointer;
          continue;
        }

        if(lock_mode == LOCK_MODE_SHARED)
        {
          if(it->lock_mode == LOCK_MODE_SHARED) 
          {
            it = it->prev_pointer;
            continue;
          }
          curr_trx->waiting_trx = it->owner_trx;

          if(trx_check_deadlock(curr_trx) == true)
          {
            throw DeadlockDetectException();
          }

          it->next_trx.push_back(curr_trx);
          it->successor_cnt++;
          pthread_cond_wait(&(it->cond), &lock_table_latch); 

          it->successor_cnt--;
          if(it->successor_cnt == 0) pthread_cond_broadcast(&(it->delete_cond));
          break;
        }
        else if(lock_mode == LOCK_MODE_EXCLUSIVE)
        {
          if(flag && it->lock_mode == LOCK_MODE_EXCLUSIVE) break;
          curr_trx->waiting_trx = it->owner_trx;

          if(trx_check_deadlock(curr_trx) == true)
          {
            throw DeadlockDetectException();
          }

          it->successor_cnt++;
          it->next_trx.push_back(curr_trx);
          pthread_cond_wait(&(it->cond), &lock_table_latch); 

          it->successor_cnt--;
          if(it->successor_cnt == 0) pthread_cond_broadcast(&(it->delete_cond));

          if(it->lock_mode == LOCK_MODE_EXCLUSIVE) break;
          else flag = 1;
        }

        auto prev = it->prev_pointer;

        it = prev;
      }    
    }
  }
  catch(const DeadlockDetectException& e)
  {
    pthread_mutex_unlock(&lock_table_latch);
    throw;
  }

  lock_object->is_acquired = true;

  pthread_cond_broadcast(&lock_object->acq_cond);
  pthread_mutex_unlock(&lock_table_latch);

  //lock_object->print();
  //puts("");

  return lock_object;
};

void remove_trx_locks(lock_t* head)
{
  if(head == nullptr) return;
  lock_t* it = head;
  lock_t* next = head;

  while(it != nullptr)
  {
    lock_release(it);
    next = it->trx_next;
    delete it;
    it = next;
  }
}

int lock_release(lock_t* lock_obj)
{
  pthread_mutex_lock(&lock_table_latch);

  lock_obj->is_end = true;

  for(Transaction* trx : lock_obj->next_trx)
  {
    trx->waiting_trx = nullptr;
  }
  pthread_cond_broadcast(&(lock_obj->cond));
  pthread_cond_broadcast(&(lock_obj->acq_cond));

  lock_list_t* lock_list =
    Lock_table.get_list(lock_obj->table_id, lock_obj->page_id);

  if(lock_obj->successor_cnt > 0)
  {
    pthread_cond_wait(&(lock_obj->delete_cond), &lock_table_latch); 
  }

  // set list
  if(lock_obj->prev_pointer)
  {
    lock_obj->prev_pointer->next_pointer = lock_obj->next_pointer;
  }
  else if(lock_list->head == lock_obj) lock_list->head = lock_obj->next_pointer;

  if(lock_obj->next_pointer)
  {
    lock_obj->next_pointer->prev_pointer = lock_obj->prev_pointer;
  }
  else if(lock_list->tail == lock_obj) lock_list->tail = lock_obj->prev_pointer;
  //printf("Release E\n");

  pthread_mutex_unlock(&lock_table_latch);
  return 0;
}
