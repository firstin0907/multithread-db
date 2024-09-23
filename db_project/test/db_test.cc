#include <gtest/gtest.h>
#include <string>
#include <stdint.h>
#include <cstring>
#include <cstdio>

#include "../include/db.h"
#include "../include/buffer.h"

class DbTest : public ::testing::Test
{
protected:
    int64_t    table_id;
    std::string pathname;
    
    DbTest()
    {
        init_db(500);

        pathname = "test_test_db.db";
        table_id = open_table(pathname.c_str());
    }

    ~DbTest()
    {
        if(table_id >= 0)
        {
            shutdown_db();
        }

        // Remove the db file
        remove(pathname.c_str());
    }
};


TEST_F(DbTest, UpdateOneRecord)
{
    // set value to be inputted into database.
    int64_t key = 108;
    const char* value = "what! a! good! day! 50bytes means 400bits e ahhhhfwiefowfweh!!!! ";
    char value2[] = "ohat! a! good! day! 50bytes means 400bits e ahhhhfwiefowfweh!!!? ";
    uint16_t len = strlen(value);

    // insert record
    int result = db_insert(table_id, key, value, len);
    ASSERT_EQ(result, 0) << "failed to insert a record.";

    result = db_update(table_id, key, value2, len, &len, 0);
    ASSERT_EQ(result, 0) << "failed to update a record.";
    
    // find record inserted and updated.
    char* finded_value = new char[len + 10];
    uint16_t finded_size; 
    result = db_find(table_id, key, finded_value, &finded_size, 0);
    ASSERT_EQ(result, 0) << "failed to find a record.";
    ASSERT_EQ(len, finded_size) << "does not match size of data written and read";

    for(int i = 0; i < len; i++)
    {
        ASSERT_EQ(value2[i], finded_value[i])
        << "difference of content between written and read has been detected at index "
        << i << "!";
    }
}

TEST_F(DbTest, WriteAndReadOneRecord)
{
    // set value to be inputted into database.
    int64_t key = 108;
    const char* value = "what! a! good! day! 50bytes means 400bits e ahhhhfwiefowfweh!!!! ";
    uint16_t len = strlen(value);

    // insert record
    int result = db_insert(table_id, key, value, len);
    ASSERT_EQ(result, 0) << "failed to insert a record.";
    
    // find record inserted.
    char* finded_value = new char[len + 10];
    uint16_t finded_size; 
    result = db_find(table_id, key, finded_value, &finded_size, 0);
    ASSERT_EQ(result, 0) << "failed to find a record.";
    ASSERT_EQ(len, finded_size) << "does not match size of data written and read";

    for(int i = 0; i < len; i++)
    {
        ASSERT_EQ(value[i], finded_value[i])
        << "difference of content between written and read has been detected at index "
        << i << "!";
    }
}


TEST_F(DbTest, WriteAndReadManyRecord)
{
    // insert
    char value[1000], finded_value[1000]; uint16_t finded_size;
    for(int64_t key = 10000; key >= -10000; key -= 2)
    {
        sprintf(value, "what! a! nice! day! 50bytes means content is record #%ld", key);
        int result = db_insert(table_id, key, value, strlen(value));
        ASSERT_EQ(result, 0) << "failed to insert a record whose key is" << key;
    }

    for(int64_t key = -9999; key <= 10000; key += 2)
    {
        sprintf(value, "what! a! nice! day! 50bytes means content is record #%ld", key);
        int result = db_insert(table_id, key, value, strlen(value));
        ASSERT_EQ(result, 0) << "failed to insert a record whose key is" << key;
    }
    
    // find record inserted.
    for(int64_t key = -10000; key <= 10000; key++)
    {
        sprintf(value, "what! a! nice! day! 50bytes means content is record #%ld", key);
        int result = db_find(table_id, key, finded_value, &finded_size, 0);
        ASSERT_EQ(result, 0) << "failed to find a record whose key is " << key;
        ASSERT_EQ(strlen(value), finded_size)
            << "does not match size of data written and read";
        
        for(int i = 0; i < finded_size; i++)
        {
            ASSERT_EQ(value[i], finded_value[i])
                << "does not match value written and read at index " << i;
        }
    }
}


TEST_F(DbTest, ScanTest)
{

    char t[300]; 
    for(int i = 1; i <= 1000; i++)
    {
        sprintf(t, "bigbewewjgioejwojigjiwejiowqddqwwqig record #%d!!", i);
        int len = strlen(t);
        db_insert(table_id, i, t, len);
    }
    
    std::vector<int64_t> keys;
    std::vector<char*> values;
    std::vector<uint16_t> val_sizes;

    int result = db_scan(table_id, 10, 39, &keys, &values, &val_sizes);
    ASSERT_EQ(result, 0) << "failed to scan records.";

    for(int i = 0; i < 30; i++)
    {
        sprintf(t, "bigbewewjgioejwojigjiwejiowqddqwwqig record #%d!!", i + 10);
        int len = strlen(t);
        
        ASSERT_EQ(keys[i], i + 10) << "failed to scan keys ";
        ASSERT_EQ(strlen(t), val_sizes[i]) << "does not match size of data written and scanned";
        for(int j = 0; j < val_sizes[i]; j++)
        {
            ASSERT_EQ(t[j], values[i][j]) << "does not match value written and read at index " << j;
        }
        delete values[i];
        
    }
    
}

TEST_F(DbTest, SequentialInsertAndDelete)
{
    char value[200], finded_value[200];
    uint16_t finded_size;
    for(int32_t i = -10000; i <= 10000; i++)
    {
        printf("%d \n", i);
        sprintf(value, "The largest record can have 112 Bytes! So I am trying to test it work well\
        even given longest record %d", i % 10);

        int result = db_insert(table_id, i, value, strlen(value));
        ASSERT_EQ(result, 0) << "failed to insert a record.";
    }

    for(int32_t i = 10000; i >= -10000; i--)
    {
        int result = db_delete(table_id, i);
        ASSERT_EQ(result, 0) << "failed to delete a record at." << i;
    }

    std::vector<int64_t> keys;
    std::vector<char*> values;
    std::vector<uint16_t> val_sizes;

    int result = db_scan(table_id, -10001, 10000, &keys, &values, &val_sizes);

    ASSERT_EQ(keys.size(), 0) << "data found";
}

TEST_F(DbTest, InsertAndHalfDeleteTest)
{
    char value[200], finded_value[200];
    uint16_t finded_size;
    for(int32_t i = -1000; i <= 10000; i += 3)
    {
        sprintf(value, "The largest record can have 112 Bytes! So I am trying to test it work well\
        even given longest record %d", i % 10);

        int result = db_insert(table_id, i, value, strlen(value));
        ASSERT_EQ(result, 0) << "failed to insert a record.";
    }

    for(int32_t i = 9999; i  >= -1000; i -= 3)
    {
        sprintf(value, "The largest record can have 112 Bytes! So I am trying to test it work well\
        even given longest record %d", i % 10);

        int result = db_insert(table_id, i, value, strlen(value));
        ASSERT_EQ(result, 0) << "failed to insert a record.";
    }
    for(int32_t i = -1001; i <= 10000; i += 3)
    {
        sprintf(value, "The largest record can have 112 Bytes! So I am trying to test it work well\
        even given longest record %d", i % 10);

        int result = db_insert(table_id, i, value, strlen(value));
        ASSERT_EQ(result, 0) << "failed to insert a record.";
    }

    for(int32_t i = -1001; i <= 10000; i++)
    {
        if(i % 100 == 3 || i / 100 == 20 || i >= 9283 || i == 2921 )
        {
            int result = db_delete(table_id, i);
            ASSERT_EQ(result, 0) << "failed to delete a record.";
        }
    }
    
    std::vector<int64_t> keys;
    std::vector<char*> values;
    std::vector<uint16_t> val_sizes;

    int result = db_scan(table_id, -1001, 10000, &keys, &values, &val_sizes);
    ASSERT_EQ(result, 0) << "failed to scan records.";

    for(int32_t i = -1001; i <= 10000; i++)
    {
        if(i % 100 == 3 || i / 100 == 20 || i >= 9283 || i == 2921 )
        {
            int result = db_delete(table_id, i);
            ASSERT_EQ(result, -1) << "it's already deleted record! at " << i;
        }
    }

    for(int i = 0; i < keys.size(); i++)
    {
        if(keys[i] % 100 == 3 || keys[i] / 100 == 20 || keys[i] >= 9283 || keys[i] == 2921 )
        {
            FAIL();
        }
        sprintf(value, "The largest record can have 112 Bytes! So I am trying to test it work well\
        even given longest record %d", (int)(keys[i] % 10));

        ASSERT_EQ(strlen(value), val_sizes[i]) << "does not match size of data written and scanned";
        for(int j = 0; j < val_sizes[i]; j++)
        {
            ASSERT_EQ(value[j], values[i][j]) << "does not match value written and read at index " << j;
        }
        delete values[i];
        
    }
    
}



TEST_F(DbTest, PreventionDuplicateCheck)
{
     // set value to be inputted into database.
    int64_t key = 108;
    const char* value = "TEST string Test string int double lfota gienfienfnienneinnefieinein!!!! ";

    uint16_t len = 74;
    ASSERT_EQ(db_insert(table_id, key, value, len), 0) << "Failed to first insert.";
    ASSERT_EQ(db_insert(table_id, key, value, len), -1) << "Failed to prevent same key insert.";
}
