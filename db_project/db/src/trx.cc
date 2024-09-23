#include "../include/trx.h"

#include <pthread.h>

#include <vector>
#include <map>

#include "../include/lock_table.h"
#include "../include/db.h"

TrxManager trx_manager;
pthread_mutex_t trx_table_latch = PTHREAD_MUTEX_INITIALIZER;

int trx_begin(void)
{
    int trx_id;

    pthread_mutex_lock(&trx_table_latch);
    trx_id = ++trx_manager.trx_count;

    Transaction* new_trx = new Transaction(trx_id);

    trx_manager.trx_table[trx_id] = new_trx;
    pthread_mutex_unlock(&trx_table_latch);
    
    return trx_id;
}

void trx_rollback(int trx_id, Transaction* trx, lock_t* head)
{
    for(auto i = trx->rollback_records.rbegin();
        i != trx->rollback_records.rend(); i++)
    {
        db_update_with_page(i->table_id, i->page_num, i->offset,
            i->prev_val, i->val_size, trx_id);
    }
}

int trx_abort(int trx_id)
{
    pthread_mutex_lock(&trx_table_latch);

    auto trx = trx_manager.trx_table[trx_id];
    trx_manager.trx_table.erase(trx_id);
    
    pthread_mutex_unlock(&trx_table_latch);
    
    trx_rollback(trx_id, trx, trx->lock_ptr);
    delete trx;
    
    return trx_id;
}

int trx_commit(int trx_id)
{
    pthread_mutex_lock(&trx_table_latch);

    delete trx_manager.trx_table[trx_id];
    trx_manager.trx_table.erase(trx_id);
    
    pthread_mutex_unlock(&trx_table_latch);
    
    return trx_id;
}

void trx_add_rollback_record(int trx_id, int64_t table_id, pagenum_t page_num,
    uint16_t offset, const char* prev_val, uint16_t val_size)
{
    pthread_mutex_lock(&trx_table_latch);
    auto trx = trx_manager.trx_table[trx_id];
    trx->rollback_records.push_back(
        {table_id, page_num, offset, prev_val, val_size}
    );
    pthread_mutex_unlock(&trx_table_latch);
}

bool dfs(Transaction* curr_trx)
{
    if(curr_trx == nullptr) return false;
    if(curr_trx->cycle_num == 1)
    {
        curr_trx->cycle_num = 0;
        return true;
    }
    
    else if(curr_trx->cycle_num == 0)
    {
        curr_trx->cycle_num = 1;
        bool result = dfs(curr_trx->waiting_trx);
        curr_trx->cycle_num = 0;
        return result;
    }

    else
    {
        return false;
    }
}

bool trx_check_deadlock(Transaction* curr_trx)
{
    bool result = 0;

    result = dfs(curr_trx);

    return result;
}


Transaction::Transaction(int trx_id)
: trx_id(trx_id), lock_ptr(nullptr), cycle_num(0), waiting_trx(nullptr)
{
    
}


Transaction::~Transaction()
{
    remove_trx_locks(lock_ptr);

    for(auto &i : rollback_records)
    {
        if(i.prev_val != nullptr) delete[] i.prev_val;
        i.prev_val = nullptr;
    }
}

TrxManager::TrxManager()
: trx_count(0)
{
    
}