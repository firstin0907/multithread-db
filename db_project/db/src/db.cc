#include "../include/bpt.h"
#include "../include/db.h"
#include "../include/file.h"
#include "../include/buffer.h"
#include "../include/trx.h"
#include "../include/lock_table.h"

#include <iostream>
#include <stdint.h>
#include <cstdio>

int64_t open_table(const char* pathname)
{
    int result = buffer_manager->open_table(pathname);

    return result;
}


int db_insert(int64_t table_id, int64_t key, const char* value,
    uint16_t val_size)
{
    try
    {
        page_t header_p;

        // get header page
        auto header_bb = buffer_manager->get_block(table_id, 0, 0, &header_p);

        // record to insert
        record new_record(key, val_size, value);

        pagenum_t root = header_p.ui64_array[3];

        // initially, checks there are already recode whose key is same with now.
        record* find_result = find_record(table_id, root, key, 0);
        if(find_result != nullptr)
        {
            // it there exists that key, delete result
            delete find_result;

            // failed to insert
            return -1;
        }

        pagenum_t new_root = insert(table_id, root, &new_record);
        if(root != new_root)
        {
            header_p.ui64_array[3] = new_root;
            buffer_manager->write_page(header_bb, header_p);
        }

        return 0;
    }
    catch(const NoSpaceException& e)
    {
        // std::cout << e.what() << std::endl;
        return -1;
    }
}

int db_find(int64_t table_id, int64_t key, char * ret_val,
    uint16_t* val_size, int trx_id)
{
    try
    {
        // get header page
        page_t header_p;
        buffer_manager->get_block(table_id, 0, trx_id, &header_p);

        // extract root page number from root
        pagenum_t root = header_p.ui64_array[3];

        // find recode
        record* rec = find_record(table_id, root, key, trx_id);

        if(rec == nullptr)
        {
            // if failure, return -1
            return -1;
        }
        else
        {
            // set value we find
            *val_size = rec->size;
            for(int i = 0; i < rec->size; i++) ret_val[i] = rec->content[i];
            
            // deallocate rec pointer
            delete[] rec->content;
            delete rec;
            
            return 0;
        }
    }
    catch(const std::exception& e)
    {
        trx_abort(trx_id);
        // std::cout << e.what() << std::endl;
        return -1;
    }
}

BufferBlockPointer update_phase_1(int64_t table_id, int64_t key, int trx_id)
{
    page_t header_p, leaf_p;

    // get header page
    auto header_bb = buffer_manager->get_block(table_id, 0,
        trx_id, &header_p);

    // extract root page number from root
    pagenum_t root = header_p.ui64_array[3];
    BufferBlockPointer leaf_bb = find_leaf(table_id, root, key, trx_id);
    if(leaf_bb.valid == false) return BufferBlockPointer::unvalid_instance();

    // get leaf page
    buffer_manager->get_page(leaf_bb, leaf_p);

    int num_keys = leaf_p.si32_array[3];
    for (int i = 0; i < num_keys; i++)
    {
        if(leaf_p.get_pos_value<int64_t>(128 + i * 12) == key)
        {
            return leaf_bb;
        }
    }

    // if there is empty tree
    return BufferBlockPointer::unvalid_instance(); 
}

int update_phase_2(int64_t table_id, pagenum_t leaf, int64_t key, char* value,
    uint16_t new_val_size, char** old_val, uint16_t* old_val_size,
    uint16_t* offset, int trx_id)
{
    page_t leaf_p;
    BufferBlockPointer leaf_bb
        = buffer_manager->get_block(table_id, leaf, trx_id, &leaf_p);
    int num_keys = leaf_p.si32_array[3], i;

    for (i = 0; i < num_keys; i++)
    {
        if(leaf_p.get_pos_value<int64_t>(128 + i * 12) == key)
        {
            *old_val_size = leaf_p.get_pos_value<uint16_t>(128 + i * 12 + 8);
            *old_val = new char[*old_val_size];
            *offset = leaf_p.get_pos_value<uint16_t>(128 + i * 12 + 10);

            for(uint16_t j = 0; j < new_val_size; j++)
            {
                (*old_val)[j] = leaf_p.c_array[j + (*offset)];
                leaf_p.c_array[j + (*offset)] = value[j];
            }
            buffer_manager->write_page(leaf_bb, leaf_p);
            // get lock
            return 0;
        }
    }
    
    return -1;
}

int db_update(int64_t table_id, int64_t key, char* value, uint16_t new_val_size,
    uint16_t* old_val_size, int trx_id)
{
    char* old_value;
    uint16_t offset;
    try
    {
        pagenum_t leaf;
        {
            BufferBlockPointer leaf_bb = update_phase_1(table_id, key, trx_id);
            if(leaf_bb.valid == 0) return -1;
            leaf = leaf_bb.page_num;
        }

        lock_acquire(table_id, leaf, key, trx_id, LOCK_MODE_EXCLUSIVE);

        int result = update_phase_2(table_id, leaf, key, value, new_val_size,
            &old_value, old_val_size, &offset, trx_id);
        // TODO
        trx_add_rollback_record(
            trx_id, table_id, leaf, offset, old_value, *old_val_size);

        return result;
    }
    catch(const std::exception& e)
    {
        // std::cout << e.what() << std::endl;
        trx_abort(trx_id);
        return -1;
    }
}

void db_update_with_page(int table_id, pagenum_t page_num, uint16_t offset,
    const char* value, uint16_t val_size, int trx_id)
{
    page_t leaf_p;
    BufferBlockPointer leaf_bb = buffer_manager->get_block(
        table_id, page_num, trx_id, &leaf_p);

    for(uint16_t i = 0; i < val_size; i++)
    {
        leaf_p.c_array[offset + i] = value[i];
    }
    buffer_manager->write_page(leaf_bb, leaf_p);
}


int db_delete(int64_t table_id, int64_t key)
{
    try
    {
        page_t header_p;
        // get header page
        auto header_bb = buffer_manager->get_block(table_id, 0, 0, &header_p);
        // extract root page number from root
        pagenum_t root = header_p.ui64_array[3];


        pagenum_t key_leaf = find_leaf(table_id, root, key, 0).page_num;
        record* key_record = find_record(table_id, root, key, 0);

        // if we find corresponding record
        if (key_record != nullptr && key_leaf != 0) {
            pagenum_t new_root = delete_entry(table_id, root, key_leaf, key, 0);
            
            // if root has been changed, write it
            if(root != new_root)
            {
                header_p.ui64_array[3] = new_root;
                buffer_manager->write_page(header_bb, header_p);
            }
            return 0;
        }
        // if we could'm find corresponding record,
        else
        {
            return -1;
        }
    }
    catch(const NoSpaceException& e)
    {
        // std::cout << e.what() << std::endl;
        return -1;
    }
}

int db_scan (int64_t table_id, int64_t begin_key, int64_t end_key, 
    std::vector<int64_t> * keys, std::vector<char*> * values,  std::vector<uint16_t> * val_sizes)
{
    try
    {
        // get header page
        page_t header_p;
        auto header_bb = buffer_manager->get_block(table_id, 0, 0, &header_p);
        // extract root page number from root
        pagenum_t root = header_p.ui64_array[3];

        find_range(table_id, root, begin_key, end_key, keys, values, val_sizes);
        return 0;
    }
    catch(const NoSpaceException& e)
    {
        // std::cout << e.what() << std::endl;
        return -1;
    }
}


int init_db(int num_buf)
{
    buffer_manager = new BufferManager(num_buf);
    return 0;
}

int shutdown_db()
{
    buffer_manager->close_tables();
    return 0;
}