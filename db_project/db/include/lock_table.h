#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

#define LOCK_MODE_SHARED    0
#define LOCK_MODE_EXCLUSIVE 1

#include <stdio.h>
#include <stdint.h>
#include <pthread.h>

#include <stdexcept>
#include <vector>

typedef struct lock_t lock_t;
typedef uint64_t pagenum_t;

struct lock_t
{
  /* GOOD LOCK :) */
  int       lock_mode;

  int64_t   table_id;
  pagenum_t page_id;
  int64_t   key;
  
  bool      is_end, is_acquired;
  uint64_t  successor_cnt;

  struct Transaction* owner_trx;
  int    owner_trx_id;

  // transactions who is wating for this lock instance
  std::vector<struct Transaction*> next_trx;

  struct lock_t* prev_pointer;
  struct lock_t* next_pointer;
  struct lock_t* trx_next;

  // conditional variable to combine with next element of the list.
  pthread_cond_t cond;

  // condition variable to delete "this" after all successor start own job.
  pthread_cond_t delete_cond;

  // condition variable to give lock to same trx
  pthread_cond_t acq_cond;

  lock_t(int lock_mode, int64_t table_id, pagenum_t page_id, int64_t key)
  : lock_mode(lock_mode), table_id(table_id), page_id(page_id), key(key)
  {
    is_end = is_acquired = false;
    successor_cnt = 0;
    
    cond         = PTHREAD_COND_INITIALIZER;
    delete_cond  = PTHREAD_COND_INITIALIZER;
    acq_cond     = PTHREAD_COND_INITIALIZER;
  };

  void init_conds()
  {
    
  }

  void print()
  {
    printf("[%c LOCK t = %ld, p = %ld, k = %ld, from %d]", 
      (lock_mode == LOCK_MODE_EXCLUSIVE) ? 'X' : 'S', table_id,
      page_id, key, owner_trx_id); 
  }
};

class DeadlockDetectException : public std::exception
{
public:
    virtual const char* what() const noexcept
    {
        return "Deadlock problem detected, so trx has been aborted.";
    }
};

extern pthread_mutex_t lock_table_latch;

/* APIs for lock table */
int init_lock_table();
lock_t* lock_acquire(int64_t table_id, pagenum_t page_id, int64_t key,
    int trx_id, int lock_mode);
void remove_trx_locks(lock_t* head);
int lock_release(lock_t* lock_obj);


#endif /* __LOCK_TABLE_H__ */
