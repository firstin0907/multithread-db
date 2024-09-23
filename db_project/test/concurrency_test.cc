#include <gtest/gtest.h>
#include <string>
#include <stdint.h>
#include <cstring>
#include <cstdio>
#include <cstdlib>

#include "../include/db.h"
#include "../include/buffer.h"
#include "../include/trx.h"

#define THREAD_NUMBER 40
#define RECORD_NUMBER 1000
#define TOTAL_RECORD_NUMBER 8000

class ConcurrencyTestWithSmallBuffer : public ::testing::Test
{
protected:
    int64_t    table_id;
    std::string pathname;
    
    ConcurrencyTestWithSmallBuffer()
    {
        init_db(100);

        pathname = "test_test_db.db";
        // Remove the db file
        remove(pathname.c_str());
        table_id = open_table(pathname.c_str());
    }

    ~ConcurrencyTestWithSmallBuffer()
    {
        if(table_id >= 0)
        {
            shutdown_db();
        }

        // Remove the db file
        remove(pathname.c_str());
    }
};


class ConcurrencyTest : public ::testing::Test
{
protected:
    int64_t    table_id;
    std::string pathname;
    
    ConcurrencyTest()
    {
        init_db(500);

        pathname = "test_test_db.db";
        remove(pathname.c_str());
        table_id = open_table(pathname.c_str());
    }

    ~ConcurrencyTest()
    {
        if(table_id >= 0)
        {
            shutdown_db();
        }

        // Remove the db file
        remove(pathname.c_str());
    }
};

bool check_all_page_latch_unlock()
{
    for(auto i = buffer_manager->buffer_list_head; i != nullptr; i = i->list_next)
    {
        if(pthread_mutex_trylock(&i->mutex) == 0)
        {
            if(i->is_pinned > 0)
            {
                return false;
            }
            pthread_mutex_unlock(&i->mutex);
        }
        else
        {
            return false;
        }
    }
    return true;
}
void* s_only_transaction(void* tid)
{
    int trx_id = trx_begin();
    int table_id = *((int*)tid);

    int test_keys[RECORD_NUMBER];
    for(int i = 0; i < RECORD_NUMBER; i++)
    {
        test_keys[i] = -10000 + (rand() % 20000);
    }

    char return_value[120];
    uint16_t return_size;
    int result = 0;
    for(int i = 0; i < RECORD_NUMBER; i++)
    {
        result = db_find(table_id, test_keys[i], return_value,
            &return_size, trx_id);
    }

    result = trx_commit(trx_id);
    return nullptr;
}

void* x_only_transaction(void* tid)
{
    int trx_id = trx_begin();
    int table_id = *((int*)tid);
    int start = *(1 + (int*)tid);

    char value[120] = "She largest record can have 112 Bytes! So I am trying to test it work well even given longest record";
    uint16_t return_size;
    int result = 0;
    for(int i = start; i < start + RECORD_NUMBER; i++)
    { 
      //  printf("%d, Start %d\n", trx_id, i);
        result = db_update(table_id, i, value, 100, &return_size, trx_id);
        if(result == -1) return nullptr;
    }

    result = trx_commit(trx_id);
    return nullptr;
}

void* x_only_reverse_transaction(void* tid)
{
    int trx_id = trx_begin();
    int table_id = *((int*)tid);
    int start = *(1 + (int*)tid);

    int test_keys[1000];

    char value[120] = "She largest record can have 112 Bytes! So I am trying to test it work well even given longest record";
    uint16_t return_size;
    int result = 0;
    for(int i = start + RECORD_NUMBER -1; i >= start; i--)
    {
        //printf("%d, Start %d\n", trx_id, i);
        result = db_update(table_id, i, value, 100, &return_size, trx_id);
        if(result == -1) return nullptr;
    }

    result = trx_commit(trx_id);
    return tid;
}

void* s_x_no_cycle_transaction(void* tid)
{
    int trx_id = trx_begin();
    int table_id = *((int*)tid);
    int start = *(1 + (int*)tid);

    char value[120];
    uint16_t return_size;
    int result = 0;
    for(int i = start; i < start + RECORD_NUMBER; i++)
    {
        result = db_find(table_id, i, value, &return_size, trx_id);
        if(result != 0) return 0;

        int oc = value[0]++;
        
        result = db_update(table_id, i, value, return_size, &return_size, trx_id);
        if(result != 0) return 0;
        
        result = db_find(table_id, i, value, &return_size, trx_id);
        if(result != 0) return 0;
    }

    result = trx_commit(trx_id);
    return nullptr;
}

void* s_x_reverse_transaction(void* tid)
{
    int trx_id = trx_begin();
    int table_id = *((int*)tid);
    int start = *(1 + (int*)tid);

    char value[120];
    uint16_t return_size;
    int result = 0;
    for(int i = start + RECORD_NUMBER - 1; i >= start; i--)
    {
        result = db_find(table_id, i, value, &return_size, trx_id);
        if(result != 0) return 0;

        if(value[0] != 'T')
        {
            puts("DEBUG");
        }

        int oc = value[0]++;
        
        result = db_update(table_id, i, value, return_size, &return_size, trx_id);
        if(result != 0) return 0;
        
        result = db_find(table_id, i, value, &return_size, trx_id);
        if(result != 0) return 0;
    }

    result = trx_commit(trx_id);
    return nullptr;
}

TEST_F(ConcurrencyTestWithSmallBuffer, SingleThreadTest)
{
    char value[120];
    int test_keys[RECORD_NUMBER];
    int answer[30000] = {0};

    // insert initial records
    for(int64_t i = -10000; i <= 10000; i++)
    {
        sprintf(value, "The largest record can have 112 Bytes! So I am trying\
            to test it work well even given longest record %ld", i % 10);

        int result = db_insert(table_id, i, value, strlen(value));
        if(result != 0) break;
    }

    puts("success to insert");
    ASSERT_EQ(check_all_page_latch_unlock(), true) << "page lock remains!";
    
    int trx_id = trx_begin();
    ASSERT_GE(trx_id, 1) << "trx_begin() returns wrong value " << trx_id;
    
    srand(3);
    for(int i = 0; i < RECORD_NUMBER; i++)
    {
        test_keys[i] = -10000 + i; //+ (rand() % 20000);
    }

    char return_value[120];
    uint16_t return_size;

    int result = 0;
    for(int i = 0; i < RECORD_NUMBER; i++)
    {
        result = db_find(table_id, test_keys[i], value,
            &return_size, trx_id);
        if(result != 0) break;

        value[0] += i % 10;
        answer[test_keys[i] + 10000] += i % 10;

        result = db_update(table_id, test_keys[i], value, return_size,
            &return_size, trx_id);        
        if(result != 0) break;
    }

    result = trx_commit(trx_id);

    ASSERT_EQ(check_all_page_latch_unlock(), true) << "page lock remains!";
    ASSERT_NE(result, 0) << "trx_commit() failed (returns " << result << ")";
}

TEST_F(ConcurrencyTest, SingleThreadTest)
{
    char value[120];
    int test_keys[RECORD_NUMBER];
    int answer[30000] = {0};

    // insert initial records
    for(int64_t i = -10000; i <= 10000; i++)
    {
        sprintf(value, "The largest record can have 112 Bytes! So I am trying\
            to test it work well even given longest record %ld", i % 10);

        int result = db_insert(table_id, i, value, strlen(value));
        ASSERT_EQ(result, 0) << "failed to insert a record.";
    }

    puts("success to insert");
    ASSERT_EQ(check_all_page_latch_unlock(), true) << "page lock remains!";
    
    int trx_id = trx_begin();
    ASSERT_GE(trx_id, 1) << "trx_begin() returns wrong value " << trx_id;
    
    srand(3);
    for(int i = 0; i < RECORD_NUMBER; i++)
    {
        test_keys[i] = -10000 + i; //+ (rand() % 20000);
    }

    char return_value[120];
    uint16_t return_size;

    int result = 0;
    for(int i = 0; i < RECORD_NUMBER; i++)
    {
        result = db_find(table_id, test_keys[i], value,
            &return_size, trx_id);
        ASSERT_EQ(result, 0) << "failed to find(1) " << test_keys[i];

        value[0] += i % 10;
        answer[test_keys[i] + 10000] += i % 10;

        result = db_update(table_id, test_keys[i], value, return_size,
            &return_size, trx_id);

        
        ASSERT_EQ(result, 0) << "failed to update " << test_keys[i];
        
    }

     for(int i = 0; i < RECORD_NUMBER; i++)
    {
        result = db_find(table_id, test_keys[i], value,
            &return_size, trx_id);

        ASSERT_EQ(result, 0) << "failed to find " << test_keys[i];
        ASSERT_EQ(value[0], 'T' + answer[test_keys[i] + 10000]) << " at " << i;
    }
    
    result = trx_commit(trx_id);

    ASSERT_EQ(check_all_page_latch_unlock(), true) << "page lock remains!";
    ASSERT_NE(result, 0) << "trx_commit() failed (returns " << result << ")";
}

TEST_F(ConcurrencyTest, SLockOnlyTest)
{
    char value[120];
    int test_keys[10000];

    // insert initial records
    for(int64_t i = -TOTAL_RECORD_NUMBER / 2; i <= TOTAL_RECORD_NUMBER / 2; i++)
    {
        sprintf(value, "The largest record can have 112 Bytes! So I am trying to test it work well even given longest record %ld", i % 10);

        int result = db_insert(table_id, i, value, strlen(value));
        ASSERT_EQ(result, 0) << "failed to insert a record.";
    }


    srand(3);

    pthread_t threads[THREAD_NUMBER];
    for (int i = 0; i < THREAD_NUMBER; i++)
    {
		pthread_create(&threads[i], 0, s_only_transaction, (void *) &table_id);
	}

    for (int i = 0; i < THREAD_NUMBER; i++)
    {
		pthread_join(threads[i], NULL);
	}    
    ASSERT_EQ(check_all_page_latch_unlock(), true) << "page lock remains!";
}

TEST_F(ConcurrencyTest, XLockOnlyTest)
{
    char value[120] = "The largest record can have 112 Bytes! So I am trying to test it work well even given longest record";

    // insert initial records
    for(int64_t i = -TOTAL_RECORD_NUMBER / 2; i <= TOTAL_RECORD_NUMBER / 2; i++)
    {
        int result = db_insert(table_id, i, value, strlen(value));
        ASSERT_EQ(result, 0) << "failed to insert a record.";
    }


    srand(3);

    pthread_t threads[THREAD_NUMBER];
    int arr[2] = {(int)table_id, -1234};
    for (int i = 0; i < THREAD_NUMBER; i++)
    {
		pthread_create(&threads[i], 0, x_only_transaction, (void *)arr);
	}

    for (int i = 0; i < THREAD_NUMBER; i++)
    {
		pthread_join(threads[i], NULL);
	}

    uint16_t val_size;
    for(int i = -1234; i < -1234 + RECORD_NUMBER; i++)
    {
        int result = db_find(table_id, i, value, &val_size, 0);
        ASSERT_EQ(result, 0) << "failed to find a record";
        ASSERT_EQ(value[0], 'S') << "wrong record at " << i;
    }
}

TEST_F(ConcurrencyTest, XLockOnlyDeadlockTest)
{
    char value[120] = "The largest record can have 112 Bytes! So I am trying to test it work well even given longest record";

    // insert initial records
    for(int64_t i = -TOTAL_RECORD_NUMBER / 2; i <= TOTAL_RECORD_NUMBER / 2; i++)
    {
        int result = db_insert(table_id, i, value, strlen(value));
        ASSERT_EQ(result, 0) << "failed to insert a record.";
    }


    srand(3);

    pthread_t threads[THREAD_NUMBER];
    int arr[2] = {(int)table_id, -1234};
    for (int i = 0; i < THREAD_NUMBER; i++)
    {
		
        if(i % 2 == 0)
        {
            pthread_create(&threads[i], 0, x_only_transaction, (void *)arr);
        }
        else
        {
            pthread_create(&threads[i], 0, x_only_reverse_transaction,
                (void *)arr);
        }
	}

    for (int i = 0; i < THREAD_NUMBER; i++)
    {
		pthread_join(threads[i], NULL);
	}

    uint16_t val_size;
    for(int i = -1234; i < -1234 + RECORD_NUMBER; i++)
    {
        int result = db_find(table_id, i, value, &val_size, 0);
        ASSERT_EQ(result, 0) << "failed to find a record";
        ASSERT_EQ(value[0], 'S') << "wrong record at " << i;
    }
}

void* xlodt1(void* tid)
{
    int trx_id = trx_begin();
    int table_id = *((int*)tid);
    
    uint16_t value_len;
    char value[120] = "The largest record can have 112 Bytes! So I am trying to test it work well even given longest record";
    printf("%d", db_update(table_id, 1000, value, value_len, &value_len, trx_id));
    sleep(3);
    printf("%d", db_update(table_id, 1001, value, value_len, &value_len, trx_id));
    printf("%d", db_update(table_id, 1001, value, value_len, &value_len, trx_id));

    return nullptr;
}

void* xlodt2(void* tid)
{
    int trx_id = trx_begin();
    int table_id = *((int*)tid);
    
    uint16_t value_len;
    char value[120] = "The largest record can have 112 Bytes! So I am trying to test it work well even given longest record";
    printf("%d", db_update(table_id, 1001, value, value_len, &value_len, trx_id));
    sleep(3);
    sleep(3);
    printf("%d", db_update(table_id, 1000, value, value_len, &value_len, trx_id));

    return nullptr;
}

TEST_F(ConcurrencyTest, XLockOnlyDeadlockTest2)
{
    char value[120] = "The largest record can have 112 Bytes! So I am trying to test it work well even given longest record";
    uint16_t value_len = (uint16_t) strlen(value);
    
    // insert initial records
    for(int64_t i = 999; i <= 1004; i++)
    {
        int result = db_insert(table_id, i, value, value_len);
        ASSERT_EQ(result, 0) << "failed to insert a record.";
    }
    puts("insert success");
    
    

    pthread_t threads[2];
    int arr[2] = {(int)table_id, -1234};
    pthread_create(&threads[0], 0, xlodt1, (void *)arr);
    pthread_create(&threads[1], 0, xlodt2, (void *)arr);
	
    pthread_join(threads[0], NULL);
    pthread_join(threads[1], NULL);


    
}


TEST_F(ConcurrencyTest, S_XLockDeadlockTest2)
{
    char value[120] = "The largest record can have 112 Bytes! So I am trying to test it work well even given longest record";

    // insert initial records
    for(int64_t i = -TOTAL_RECORD_NUMBER / 2; i <= TOTAL_RECORD_NUMBER / 2; i++)
    {
        int result = db_insert(table_id, i, value, strlen(value));
        ASSERT_EQ(result, 0) << "failed to insert a record.";
    }


    srand(3);

    pthread_t threads[2];
    int arr[2] = {(int)table_id, -1234};
    for (int i = 0; i < 2; i++)
    {
		
        if(i % 2 == 0)
        {
            pthread_create(&threads[i], 0, s_x_no_cycle_transaction, (void *)arr);
        }
        else
        {
            pthread_create(&threads[i], 0, s_x_reverse_transaction,
                (void *)arr);
        }
	}

    for (int i = 0; i < 2; i++)
    {
		pthread_join(threads[i], NULL);
	}

    uint16_t val_size;
    puts("");
    for(int i = -1234; i < -1234 + RECORD_NUMBER; i++)
    {
        int result = db_find(table_id, i, value, &val_size, 0);
        printf("%c", value[0]);
    }
    puts("");
    for(int i = -1234; i < -1234 + RECORD_NUMBER; i++)
    {
        int result = db_find(table_id, i, value, &val_size, 0);
        ASSERT_EQ(result, 0) << "failed to find a record";
        ASSERT_EQ(value[0], 'T' + 1) << "wrong record at " << i;
    }
}

TEST_F(ConcurrencyTest, BothLockTest)
{
    char value[120] = "012 largest record can have 112 Bytes! So I am trying to test it work well even given longest record";
    int test_keys[10000];

    // insert initial records
    for(int64_t i = -1234; i <= -1234 + RECORD_NUMBER; i++)
    {
        int result = db_insert(table_id, i, value, strlen(value));
        ASSERT_EQ(result, 0) << "failed to insert a record.";
    }


    srand(3);

    pthread_t threads[THREAD_NUMBER];
    int arr[2] = {table_id, -1234};
    for (int i = 0; i < THREAD_NUMBER; i++)
    {
		pthread_create(&threads[i], 0, s_x_no_cycle_transaction, (void *)arr);
	}

    for (int i = 0; i < THREAD_NUMBER; i++)
    {
		pthread_join(threads[i], NULL);
	}

    uint16_t val_size;
    for(int i = -1234; i < -1234 + RECORD_NUMBER; i++)
    {
        int result = db_find(table_id, i, value, &val_size, 0);
        ASSERT_EQ(result, 0) << "failed to find a record at" << i;
    //    ASSERT_EQ(value[0], '0' + THREAD_NUMBER) << "wrong record at " << i;
    }
}
