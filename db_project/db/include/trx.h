#include <pthread.h>

#include <stdint.h>

#include <vector>
#include <map>

typedef uint64_t pagenum_t;

struct rollback_record_t
{
    int64_t     table_id;
    pagenum_t   page_num;
    uint16_t    offset;

    const char* prev_val;
    uint16_t    val_size;
};

struct Transaction
{
    int trx_id;
    int cycle_num;
    struct lock_t* lock_ptr;
    std::vector<rollback_record_t> rollback_records;
    
    struct Transaction* waiting_trx;

    Transaction(int trx_id);
    ~Transaction();
};

struct TrxManager
{
    int trx_count;
    std::map<int, Transaction*> trx_table;

    TrxManager();
};


int trx_begin(void);

int trx_abort(int trx_id);

int trx_commit(int trx_id);

void trx_add_rollback_record(int trx_id, int64_t table_id, pagenum_t page_num,
    uint16_t offset, const char* prev_val, uint16_t val_size);

// if deadlock detected, return true
bool trx_check_deadlock(Transaction* curr_trx);

extern TrxManager trx_manager;
extern pthread_mutex_t trx_table_latch;