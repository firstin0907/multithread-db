/*
 *  bpt.c  
 */
#define Version "1.14"
/*
 *
 *  bpt:  B+ Tree Implementation
 *  Copyright (C) 2010-2016  Amittai Aviram  http://www.amittai.com
 *  All rights reserved.
 *  Redistribution and use in source and binary forms, with or without
 *  modification, are permitted provided that the following conditions are met:
 *
 *  1. Redistributions of source code must retain the above copyright notice, 
 *  this list of conditions and the following disclaimer.
 *
 *  2. Redistributions in binary form must reproduce the above copyright notice, 
 *  this list of conditions and the following disclaimer in the documentation 
 *  and/or other materials provided with the distribution.
 
 *  3. Neither the name of the copyright holder nor the names of its 
 *  contributors may be used to endorse or promote products derived from this 
 *  software without specific prior written permission.
 
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE 
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE 
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE 
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR 
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF 
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN 
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 *  POSSIBILITY OF SUCH DAMAGE.
 
 *  Author:  Amittai Aviram 
 *    http://www.amittai.com
 *    amittai.aviram@gmail.edu or afa13@columbia.edu
 *  Original Date:  26 June 2010
 *  Last modified: 17 June 2016
 *
 *  This implementation demonstrates the B+ tree data structure
 *  for educational purposes, includin insertion, deletion, search, and display
 *  of the search path, the leaves, or the whole tree.
 *  
 *  Must be compiled with a C99-compliant C compiler such as the latest GCC.
 *
 *  Usage:  bpt [order]
 *  where order is an optional argument
 *  (integer MIN_ORDER <= order <= MAX_ORDER)
 *  defined as the maximal number of pointers in any node.
 *
 */

#include "../include/bpt.h"

#include <iostream>
#include <cstdio>
#include <memory>

#include <algorithm>

#include "../include/file.h"
#include "../include/buffer.h"
#include "../include/lock_table.h"
// GLOBALS.

/* The order determines the maximum and minimum
 * number of entries (keys and pointers) in any
 * node.  Every node has at most order - 1 keys and
 * at least (roughly speaking) half that number.
 * Every leaf has as many pointers to data as keys,
 * and every internal node has one more pointer
 * to a subtree than the number of keys.
 * This global variable is initialized to the
 * default value.
 */
int order = DEFAULT_ORDER;
int real_order = 124;


// FUNCTION DEFINITIONS.

// OUTPUT AND UTILITIES

/* Copyright and license notice to user at startup. 
 */

record::record(int64_t key, uint16_t size, const char* content)
    : key(key), size(size), content(content) {};

void license_notice( void ) {
    printf("bpt version %s -- Copyright (C) 2010  Amittai Aviram "
            "http://www.amittai.com\n", Version);
    printf("This program comes with ABSOLUTELY NO WARRANTY; for details "
            "type `show w'.\n"
            "This is free software, and you are welcome to redistribute it\n"
            "under certain conditions; type `show c' for details.\n\n");
}


/* Routine to print portion of GPL license to stdout.
 */
void print_license( int license_part ) {
    int start, end, line;
    FILE * fp;
    char buffer[0x100];

    switch(license_part) {
    case LICENSE_WARRANTEE:
        start = LICENSE_WARRANTEE_START;
        end = LICENSE_WARRANTEE_END;
        break;
    case LICENSE_CONDITIONS:
        start = LICENSE_CONDITIONS_START;
        end = LICENSE_CONDITIONS_END;
        break;
    default:
        return;
    }

    fp = fopen(LICENSE_FILE, "r");
    if (fp == NULL) {
        perror("print_license: fopen");
        exit(EXIT_FAILURE);
    }
    for (line = 0; line < start; line++)
        fgets(buffer, sizeof(buffer), fp);
    for ( ; line < end; line++) {
        fgets(buffer, sizeof(buffer), fp);
        printf("%s", buffer);
    }
    fclose(fp);
}



/* Finds keys and their pointers, if present, in the range specified
 * by key_start and key_end, inclusive.  Places these in the arrays
 * returned_keys and returned_pointers, and returns the number of
 * entries found.
 */
int find_range(int64_t table_id, pagenum_t root, int64_t key_start, int64_t key_end,
    std::vector<int64_t> * keys, std::vector<char*> * values, std::vector<uint16_t> * val_sizes)
{
    int i, num_found;
    num_found = 0;

    BufferBlockPointer n_bb = find_leaf(table_id, root, key_start, 0);
    pagenum_t n = n_bb.page_num;
    page_t n_p;

    if(n == 0) return 0;

    while(n != 0)
    {
        buffer_manager->get_block(table_id, n_bb.page_num, 0, &n_p);
        
        for(i = 0; i < n_p.si32_array[3]; i++)
        {
            auto c_key = n_p.get_pos_value<int64_t>(128 + 12 * i);
            if(key_end < c_key) return num_found;
            if(key_start <= c_key)
            {
                auto c_size = n_p.get_pos_value<uint16_t>(128 + 8 + 12 * i);
                auto c_offset = n_p.get_pos_value<uint16_t>(128 + 10 + 12 * i);
                auto value = new char[c_size];
                for(uint16_t j = 0; j < c_size; j++)
                {
                    value[j] = n_p.c_array[c_offset + j];
                }

                keys->push_back(c_key);
                values->push_back(value);
                val_sizes->push_back(c_size);
            }
        }
        n = n_p.ui64_array[15];

        if(n != 0) n_bb = buffer_manager->get_block(table_id, n, 0, &n_p);
    }

    return num_found;
}


/* Traces the path from the root to a leaf, searching
 * by key.  Displays information about the path
 * if the verbose flag is set.
 * Returns the leaf containing the given key.
 */
BufferBlockPointer find_leaf(int64_t table_id, pagenum_t root, int64_t key, int trx_id)
{
    int i;
    uint32_t curr_num_keys;

    pagenum_t curr = root;
    if(curr == 0)
    {
        BufferBlockPointer p(nullptr, 0, 0);
        p.valid = 0;
        return p;
    }


    page_t curr_p;
    // read root page
    BufferBlockPointer curr_bb = buffer_manager->get_block(
        table_id, root, trx_id, &curr_p);

    // while current page is not leaf node,
    while(curr_p.ui32_array[2] != 1)
    {  
        i = 0;
        curr_num_keys = curr_p.ui32_array[3];
        
        int64_t* key_pagenum_pair = curr_p.si64_array + 128 / 8;
        
        while (i < curr_num_keys && i < real_order* 2) {
            if (key >= key_pagenum_pair[i * 2]) i++;
            else break;
        }

        curr = key_pagenum_pair[i * 2 - 1];
        curr_bb = buffer_manager->get_block(table_id, curr, trx_id, &curr_p);
    }
    
    return curr_bb;
}

/* Finds and returns the record to which
 * a key refers.
 */
record* find_record(int64_t table_id, pagenum_t root, int64_t key, int trx_id)
{
    pagenum_t leaf;
    page_t leaf_p;
    {
        BufferBlockPointer leaf_bb = find_leaf(table_id, root, key, trx_id);
        leaf = leaf_bb.page_num;

        // if there is empty tree
        if(leaf_bb.valid == false) return nullptr;
    }

    if(trx_id > 0)
        lock_acquire(table_id, leaf, key, trx_id, LOCK_MODE_SHARED);  
    
    BufferBlockPointer leaf_bb = buffer_manager->get_block(table_id, leaf,
        trx_id, &leaf_p);
    
    int num_keys = leaf_p.si32_array[3], i;
    for (i = 0; i < num_keys; i++)
    {
        if(leaf_p.get_pos_value<int64_t>(128 + i * 12) == key)
        {
            record* new_record = new record(key,
                leaf_p.get_pos_value<uint16_t>(128 + i * 12 + 8), nullptr);     
            uint16_t offset = leaf_p.get_pos_value<uint16_t>(128 + i * 12 + 10);
            
            char* content = new char[new_record->size];
            for(int j = 0; j < new_record->size; j++)
            {
                content[j] = leaf_p.c_array[j + offset];
            }
            new_record->content = content;
            return new_record;
        }
    }
    return nullptr;
}

/* Finds the appropriate place to
 * split a node that is too big into two.
 */
int cut( int length ) {
    if (length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}


// INSERTION

/* Inserts a new pointer to a record and its corresponding
 * key into a leaf.
 * Returns the altered leaf.
 */
bool insert_into_leaf(page_t* leaf, const record* src) {

    if(leaf->ui64_array[112 / 8] < 12 + src->size) return false;

    int i, insertion_point;
    int num_keys = leaf->ui32_array[3];
    

    insertion_point = 0;

    while (insertion_point < num_keys && leaf->get_pos_value<int64_t>(
        128 + insertion_point * 12) < src->key) insertion_point++;
    

    for (i = num_keys; i > insertion_point; i--) {
        leaf->get_pos_value<int64_t>(128 + i * 12)
            = leaf->get_pos_value<int64_t>(128 + (i - 1) * 12);
        
        leaf->get_pos_value<int32_t>(128 + 8 + i * 12)
            = leaf->get_pos_value<int32_t>(128 + 8 + (i - 1) * 12);
    }

    uint16_t insert_offset = 128 + num_keys * 12
        + leaf->ui64_array[112 / 8] - src->size;

    leaf->get_pos_value<int64_t>(128 + insertion_point * 12) = src->key;
    leaf->get_pos_value<uint16_t>(128 + insertion_point * 12 + 8) = src->size; // size
    leaf->get_pos_value<uint16_t>(128 + insertion_point * 12 + 10) = insert_offset;

    for(i = 0; i < src->size; i++)
    {
        leaf->c_array[insert_offset + i] = src->content[i];
    }
    leaf->ui64_array[112 / 8] -= 12 + src->size;
    leaf->ui32_array[3] += 1;

    return true;
}


/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
pagenum_t insert_into_leaf_after_splitting(
    int64_t table_id, pagenum_t root, pagenum_t leaf, const record* src) {

    int insertion_index, split, i, j;

    page_t old_leaf_p, old_leaf_clone;
    BufferBlockPointer old_leaf_bb = buffer_manager->get_block(
        table_id, leaf, 0, &old_leaf_p);
    BufferBlockPointer new_leaf_bb = buffer_manager->get_new_block(table_id, 0);
    pagenum_t new_leaf = new_leaf_bb.page_num;

    old_leaf_clone = old_leaf_p;

    page_t left_p(LEAF_PAGE), right_p(LEAF_PAGE);
 
    int num_keys = old_leaf_p.ui32_array[3];    // number of keys old leaf has.

    insertion_index = 0;
    while (insertion_index < num_keys && old_leaf_p.get_pos_value<int64_t>(
        128 + insertion_index * 12) < src->key) insertion_index++;
    
    int64_t* temp_keys = new int64_t[num_keys + 1];
    uint16_t* temp_length = new uint16_t[num_keys + 1];
    uint16_t* temp_offset = new uint16_t[num_keys + 1];

    for(i = 0, j = 0; i < num_keys; i++, j++)
    {
        if (j == insertion_index) j++;
        temp_keys[j] = old_leaf_p.get_pos_value<int64_t>(128 + i * 12); 
        temp_length[j] = old_leaf_p.get_pos_value<int16_t>(128 + i * 12 + 8); 
        temp_offset[j] = old_leaf_p.get_pos_value<int16_t>(128 + i * 12 + 10); 
    }

    temp_keys[insertion_index] = src->key; 
    temp_length[insertion_index] = src->size;
    temp_offset[insertion_index] = 0;

    uint16_t acc_size = 0;
    int64_t new_key = 0;
    for(i = 0; i < num_keys + 1 && acc_size < 1984; acc_size += temp_length[++i] + 12)
    {
        record inserted(temp_keys[i], temp_length[i],
            (temp_offset[i] == 0) ? src->content : old_leaf_p.c_array + temp_offset[i]);
        insert_into_leaf(&left_p, &inserted); 
    }
    new_key = temp_keys[i];

    for(; i < num_keys + 1; i++)
    {
        record inserted(temp_keys[i], temp_length[i], old_leaf_p.c_array + temp_offset[i]);
        if(temp_offset[i] == 0) inserted.content = src->content;
        insert_into_leaf(&right_p, &inserted); 
    }
    

    // set parent
    left_p.ui64_array[0] = right_p.ui64_array[0] = old_leaf_p.ui64_array[0];

    // set slbling
    right_p.ui64_array[15] = old_leaf_p.ui64_array[15];
    left_p.ui64_array[15] = new_leaf;

    // write
    buffer_manager->write_page(old_leaf_bb, left_p);
    buffer_manager->write_page(new_leaf_bb, right_p);

    // free arrays
    delete[] temp_keys;
    delete[] temp_length;
    delete[] temp_offset;

    try
    {
        auto new_root = insert_into_parent(table_id, root,
            old_leaf_p.ui64_array[0], leaf, new_key, new_leaf);

        return new_root;
    }
    catch(const NoSpaceException& e)
    {
        buffer_manager->set_delete_waited(new_leaf_bb);
        buffer_manager->write_page(old_leaf_bb, old_leaf_clone);

        throw e;
    }
}


/* Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
bool insert_into_node(int64_t table_id, pagenum_t n, int64_t key, pagenum_t right)
{
    
    page_t n_p, right_p;
    
    auto n_bb = buffer_manager->get_block(table_id, n, 0, &n_p);
    auto right_bb = buffer_manager->get_block(table_id, right, 0, &right_p);

    int num_keys = static_cast<int>(n_p.ui32_array[3]);
    if(num_keys >= real_order * 2) return false;
    
    int i;
    for(i = num_keys - 1; n_p.si64_array[16 + 2 * i] > key && i >= 0; i--)
    {
        // key
        n_p.si64_array[16 + 2 * (i + 1)] = n_p.si64_array[16 + 2 * i];
        
        // pagenum
        n_p.ui64_array[17 + 2 * (i + 1)] = n_p.ui64_array[17 + 2 * i];
    }
    
    n_p.si64_array[16 + 2 * (i + 1)] = key;
    n_p.ui64_array[17 + 2 * (i + 1)] = right;
    
    n_p.ui32_array[3]++;
    
    right_p.ui64_array[0] = n;

    buffer_manager->write_page(n_bb, n_p);
    buffer_manager->write_page(right_bb, right_p);

    return true;
}


/* Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
pagenum_t insert_into_node_after_splitting(int64_t table_id, pagenum_t root,
    pagenum_t old_node, int64_t key, pagenum_t right)
{
    page_t old_p, old_clone;
    page_t left_p(INTERNAL_PAGE), right_p(INTERNAL_PAGE), child_p;

    auto old_bb = buffer_manager->get_block(table_id, old_node, 0, &old_p);
    auto new_bb = buffer_manager->get_new_block(table_id, 0);

    old_clone = old_p;

    int num_keys = static_cast<int>(old_p.ui32_array[3]);
    int64_t* temp_keys = new int64_t[num_keys + 1];
    pagenum_t* temp_pointers = new pagenum_t[num_keys + 2];

    int i, j, flag = 0, insert_point = 0;

    temp_pointers[0] = old_p.ui64_array[15];
    for(i = 0, j = 0; i < num_keys; i++, j++)
    {
        if (flag == 0 && key < old_p.si64_array[16 + i * 2])
        {
            insert_point = j;
            flag = 1;
            j++;
        }
        temp_keys[j] = old_p.si64_array[16 + i * 2];
        temp_pointers[j + 1] = old_p.ui64_array[17 + i * 2];
    }
    if(flag == 0) insert_point = num_keys;
    
    temp_keys[insert_point] = key;
    temp_pointers[insert_point + 1] = right;  

    int split = cut(num_keys);
    for(i = 0; i < split - 1; i++)
    {
        left_p.ui64_array[15 + i * 2] = temp_pointers[i];
        left_p.si64_array[16 + i * 2] = temp_keys[i];
        left_p.ui32_array[3]++;
    }
    left_p.ui64_array[15 + i * 2] = temp_pointers[i];

    int64_t k_prime = temp_keys[i];

    for (++i, j = 0; i < num_keys + 1; i++, j++)
    {
        right_p.ui64_array[15 + j * 2] = temp_pointers[i];
        right_p.si64_array[16 + j * 2] = temp_keys[i];
        right_p.ui32_array[3]++;
    }
    right_p.ui64_array[15 + j * 2] = temp_pointers[i];

    delete[] temp_keys;
    delete[] temp_pointers;

    // set parent
    left_p.ui64_array[0] = right_p.ui64_array[0] = old_p.ui64_array[0];

    // write
    buffer_manager->write_page(old_bb, left_p);
    buffer_manager->write_page(new_bb, right_p);

    std::vector<std::pair<BufferBlockPointer, pagenum_t> > v;
    try 
    {
         //set parent of child
        if(insert_point < split - 1)
        {
            //if it become left_page element,
            auto child_bb = buffer_manager->get_block(table_id, right, 0, &child_p);
            v.push_back({child_bb, child_p.ui64_array[0]});

            child_p.ui64_array[0] = old_node;
            buffer_manager->write_page(child_bb, child_p);
        }
        for(int i = 0; i <= right_p.ui32_array[3]; i++)
        {
            const pagenum_t child = right_p.ui64_array[15 + i * 2];
            auto child_bb = buffer_manager->get_block(table_id, child, 0, &child_p);
            v.push_back({child_bb, child_p.ui64_array[0]});

            child_p.ui64_array[0] = new_bb.page_num;
            buffer_manager->write_page(child_bb, child_p);
        }

        return insert_into_parent(table_id, root, old_p.ui64_array[0],
            old_node, k_prime, new_bb.page_num);
    }
    catch(const NoSpaceException& e)
    {
        buffer_manager->write_page(old_bb, old_clone);
        buffer_manager->set_delete_waited(new_bb);
        
        for(auto i = v.begin(); i != v.end(); i++)
        {
            buffer_manager->get_page(i->first, child_p);
            child_p.ui64_array[0] = i->second;
            buffer_manager->write_page(i->first, child_p);
        }
        throw e;
    }
}



/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
pagenum_t insert_into_parent(int64_t table_id, pagenum_t root, pagenum_t parent,
    pagenum_t left, int64_t key, pagenum_t right)  
{

    int left_index;

    /* Case: new root. */
    if (parent == 0) return insert_into_new_root(table_id, left, key, right);

    /* Case: leaf or node. (Remainder of
     * function body.)  
     */

    /* Find the parent's pointer to the left 
     * node.
     */

    /* Simple case: the new key fits into the node. 
     */

    if(insert_into_node(table_id, parent, key, right) == true)
    {
        return root;
    }

    /* Harder case:  split a node in order 
     * to preserve the B+ tree properties.
     */

    return insert_into_node_after_splitting(table_id, root, parent, key, right);
}


/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
pagenum_t insert_into_new_root(int64_t table_id, pagenum_t left,
    int64_t key, pagenum_t right) {

    BufferBlockPointer root_bb = buffer_manager->get_new_block(table_id, 0);
    page_t root_p(INTERNAL_PAGE), left_p, right_p;
    try
    {
        auto left_bb = buffer_manager->get_block(table_id, left, 0, &left_p);
        auto right_bb = buffer_manager->get_block(table_id, right, 0, &right_p);

        left_p.ui64_array[0] = right_p.ui64_array[0] = root_bb.page_num;

        buffer_manager->write_page(left_bb, left_p);
        buffer_manager->write_page(right_bb, right_p);
    }
    catch (const NoSpaceException& e)
    {
        buffer_manager->set_delete_waited(root_bb);
        throw NoSpaceException();
    }
    
    root_p.ui32_array[3] = 1;
    root_p.ui64_array[15] = left;
    root_p.si64_array[16] = key;
    root_p.ui64_array[17] = right;

    buffer_manager->write_page(root_bb, root_p);

    

    return root_bb.page_num;
}



/* First insertion:
 * start a new tree.
 */
pagenum_t start_new_tree(int64_t table_id, const record* src)
{
    BufferBlockPointer root = buffer_manager->get_new_block(table_id, 0);
    page_t root_p(LEAF_PAGE);

    insert_into_leaf(&root_p, src);
    buffer_manager->write_page(root, root_p);
    
    return root.page_num;
}



/* Master insertion function.
 * Inserts a key and an associated value into
 * the B+ tree, causing the tree to be adjusted
 * however necessary to maintain the B+ tree
 * properties.
 */
pagenum_t insert(int64_t table_id, pagenum_t root, const record* src)
{
    /* Case: the tree does not exist yet.
     * Start a new tree.
     */
    if (root == 0) return start_new_tree(table_id, src);


    /* Case: the tree already exists.
     * (Rest of function body.)
     */

    page_t leaf_clone, leaf_p;
    BufferBlockPointer leaf_bb = find_leaf(table_id, root, src->key, 0);

    buffer_manager->get_page(leaf_bb, leaf_clone);
    leaf_p = leaf_clone;

    /* Case: leaf has room for key and pointer.
     */

    if (insert_into_leaf(&leaf_p, src) == true) {
        // if there is enough empty space on page,
        buffer_manager->write_page(leaf_bb, leaf_p);
        return root;
    }

    /* Case:  leaf must be split.
     */
    try
    {
        pagenum_t new_root = insert_into_leaf_after_splitting(
            table_id, root, leaf_bb.page_num, src);
        
        return new_root;
    }
    catch(const NoSpaceException& e)
    {
        buffer_manager->write_page(leaf_bb, leaf_clone);
        throw e;
    }
}



// DELETION.

/* Utility function for deletion.  Retrieves
 * the index of a node's nearest neighbor (sibling)
 * to the left if one exists.  If not (the node
 * is the leftmost child), returns -1 to signify
 * this special case.
 */
int get_neighbor_index(const page_t* parent_p, pagenum_t n)
{
    uint32_t num_keys = parent_p->ui32_array[3]; 
    int i;

    /* Return the index of the key to the left
     * of the pointer in the parent pointing
     * to n.  
     * If n is the leftmost child, this means
     * return -1.
     */
    for (i = 0; i <= num_keys; i++)
        if (parent_p->ui64_array[15 + i * 2] == n)
            return i - 1;

    // Error state.
    printf("Search for nonexistent pointer to node in parent.\n");
    printf("Node:  %#lx\n", (unsigned long)n);
    exit(EXIT_FAILURE);
}


void remove_entry_from_node(page_t* n_p, int64_t key, pagenum_t child)
{
    uint32_t i, num_keys = n_p->ui32_array[3];
    if(n_p->ui32_array[2] == 0)
    {
        // case:: n_p is internal node
        i = 0;
        while (n_p->si64_array[16 + i * 2] != key) i++;
        for(++i; i < num_keys; i++)
        {
            n_p->si64_array[16 + (i - 1) * 2] = n_p->si64_array[16 + i * 2];
        }

        i = 0;
        while (n_p->si64_array[15 + i * 2] != child) i++;
        for(++i; i < num_keys + 1; i++)
        {
            n_p->si64_array[15 + (i - 1) * 2] = n_p->si64_array[15 + i * 2];
        }
        n_p->si64_array[16 + (num_keys - 1) * 2] = 0;
        n_p->si64_array[17 + (num_keys - 1) * 2] = 0;
    }
    else
    {
        // case :: n_p is leaf node
        // find target to be deleted
        i = 0;
        while (n_p->get_pos_value<int64_t>(128 + i * 12) != key) i++;

        uint64_t shift_s = 128 + 12 * num_keys + n_p->ui64_array[14];
        uint64_t shift_e = n_p->get_pos_value<uint16_t>(128 + 10 + i * 12);
        uint16_t shift_scale = n_p->get_pos_value<uint16_t>(128 + 8 + i * 12);

        // shift slots
        for(++i; i < num_keys; i++)
        {
            n_p->get_pos_value<int64_t>(128 + (i - 1) * 12) = n_p->get_pos_value<int64_t>(128 + i * 12);
            n_p->get_pos_value<int32_t>(128 + (i - 1) * 12 + 8) = n_p->get_pos_value<int32_t>(128 + i * 12 + 8);
        }

        // shift values
        for(uint64_t i = shift_e - 1; i >= shift_s; i--)
        {
            n_p->c_array[i + shift_scale] = n_p->c_array[i];
        }

        // modify offset of slots
        for(i = 0; i < num_keys - 1; i++)
        {
            if(n_p->get_pos_value<uint16_t>(128 + 10 + i * 12) < shift_e)
            {
                n_p->get_pos_value<uint16_t>(128 + 10 + i * 12) += shift_scale;
            }
        }
        n_p->ui64_array[14] += 12 + shift_scale;
    }
    n_p->ui32_array[3] -= 1;
}


pagenum_t adjust_root(int64_t table_id, pagenum_t root,
    BufferBlockPointer root_bb)
{
    /* Case: nonempty root.
     * Key and pointer have already been deleted,
     * so nothing to be done.
     */

    page_t root_p;
    buffer_manager->get_block(table_id, root, 0, &root_p);

    if(root_p.ui32_array[3] > 0)
    {
        return root;
    }

    /* Case: empty root. It need to be deleted.
     */
    buffer_manager->set_delete_waited(root_bb);

    // If it has a child, promote 
    // the first (only) child
    // as the new root.

    if(root_p.ui32_array[2] == 0)
    {
        page_t new_root_p;
        pagenum_t new_root = root_p.ui64_array[15];
        BufferBlockPointer new_root_bb =
            buffer_manager->get_block(table_id, new_root, 0, &new_root_p);
        new_root_p.ui64_array[0] = 0;
        buffer_manager->write_page(new_root_bb, new_root_p);

        return new_root;
    }

    // If it is a leaf (has no children),
    // then the whole tree is empty.
    else
    {
        return 0;
    }
}


/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
pagenum_t coalesce_nodes(int64_t table_id, pagenum_t root, BufferBlockPointer n_bb,
    BufferBlockPointer neighbor_bb, int neighbor_index, int64_t k_prime)
{
    int i, j;
    uint32_t neighbor_insertion_index;
    uint32_t n_end;

    /* Swap neighbor with node if node is on the
     * extreme left and neighbor is to its right.
     */

    if (neighbor_index == -1) {
        std::swap(n_bb, neighbor_bb);
    }

    page_t n_clone, neighbor_clone;
    buffer_manager->get_page(n_bb, n_clone);
    buffer_manager->get_page(neighbor_bb, neighbor_clone);

    page_t n_p = n_clone, neighbor_p = neighbor_clone;

    /* Starting point in the neighbor for copying
     * keys and pointers from n.
     * Recall that n and neighbor have swapped places
     * in the special case of n being a leftmost child.
     */

    neighbor_insertion_index = neighbor_p.ui32_array[3];
    
    try
    {
       /* Case:  nonleaf node.
        * Append k_prime and the following pointer.
        * Append all pointers and keys from the neighbor.
        */

        if (neighbor_p.si32_array[2] == 0) {

            /* Append k_prime.
            */

            neighbor_p.si64_array[16 + neighbor_insertion_index * 2] = k_prime;
            neighbor_p.ui32_array[3] += 1;

            n_end = n_p.ui32_array[3];


            for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++)
            {
                neighbor_p.si64_array[16 + i * 2] = n_p.si64_array[16 + j * 2];
                neighbor_p.ui64_array[15 + i * 2] = n_p.ui64_array[15 + j * 2];

                neighbor_p.ui32_array[3] += 1;

                n_p.ui32_array[3] -= 1;

            }
            /* The number of pointers is always
            * one more than the number of keys.
            */
            neighbor_p.ui64_array[15 + i * 2] = n_p.ui64_array[15 + j * 2];

            /* All children must now point up to the same parent.
            */
            std::vector<std::pair<BufferBlockPointer, pagenum_t>> v;

            try
            {
                for (i = 0; i < neighbor_p.ui32_array[3] + 1; i++) {
                    pagenum_t child = neighbor_p.ui64_array[15 + i * 2];
                    page_t child_p;
                    BufferBlockPointer child_bb =
                        buffer_manager->get_block(table_id, child, 0, &child_p);
                    
                    v.push_back({child_bb, child_p.ui64_array[0]});
                    child_p.ui64_array[0] = neighbor_bb.page_num;

                    buffer_manager->write_page(child_bb, child_p);
                }
            }
            catch(const NoSpaceException &e)
            {
                for(auto it = v.begin(); it != v.end(); it++)
                {
                    page_t child_p;
                    buffer_manager->get_page(it->first, child_p);
                    child_p.ui64_array[0] = it->second;
                    buffer_manager->write_page(it->first, child_p);
                }

                throw e;
            }

            

        }

        /* In a leaf, append the keys and pointers of
        * n to the neighbor.
        * Set the neighbor's last pointer to point to
        * what had been n's right neighbor.
        */

        else {
            for (i = neighbor_insertion_index, j = 0; j < n_p.ui32_array[3]; i++, j++) {
                record rec(n_p.get_pos_value<int64_t>(128 + j * 12),
                    n_p.get_pos_value<int16_t>(128 + 8 + j * 12),
                    n_p.c_array + n_p.get_pos_value<int16_t>(128 + 10 + j * 12));
                insert_into_leaf(&neighbor_p, &rec);
            }
            neighbor_p.ui64_array[15] = n_p.ui64_array[15];
        }

        buffer_manager->write_page(n_bb, n_p);
        buffer_manager->write_page(neighbor_bb, neighbor_p);
    

        root = delete_entry(table_id, root, neighbor_p.ui64_array[0],
            k_prime, n_bb.page_num);


        buffer_manager->set_delete_waited(n_bb);

        return root;
    }
    catch(NoSpaceException& e)
    {
        buffer_manager->write_page(n_bb, n_clone);
        buffer_manager->write_page(neighbor_bb, neighbor_clone);
        
        throw e;
    }
    
}


/* Redistributes entries between two nodes when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small node's entries without exceeding the
 * maximum
 */
pagenum_t redistribute_nodes(int64_t table_id, pagenum_t root,
    BufferBlockPointer n_bb, BufferBlockPointer neighbor_bb,
    int neighbor_index, int k_prime_index, int64_t k_prime)
{
    int i;

    page_t n_p, neighbor_p, parent_p;

    buffer_manager->get_page(n_bb, n_p);
    buffer_manager->get_page(neighbor_bb, neighbor_p);

    BufferBlockPointer parent_bb = buffer_manager->get_block(
        table_id, n_p.ui64_array[0], 0, &parent_p);

    page_t n_clone = n_p, neighbor_clone = neighbor_p, parent_clone = parent_p;

    std::vector<std::pair<BufferBlockPointer, uint64_t> > v;
    

    while((n_p.ui32_array[2] == 0 && n_p.ui32_array[3] < real_order)
        || ( n_p.ui32_array[2] == 1 && n_p.si64_array[14] >= 2500))
    {
        /* Case: n has a neighbor to the left. 
        * Pull the neighbor's last key-pointer pair over
        * from the neighbor's right end to n's left end.
        */
        uint32_t n_num_keys = n_p.ui32_array[3];
        uint32_t neighbor_num_keys = neighbor_p.ui32_array[3];

        if (neighbor_index != -1)
        {
            if(n_p.ui32_array[2] == 0)
            {
                // shift n_p's element
                n_p.ui64_array[15 + (n_num_keys + 1) * 2]
                    = n_p.ui64_array[15 + (n_num_keys) * 2];
                for(i = n_num_keys; i > 0; i--)
                {
                    n_p.ui64_array[15 + i * 2] = n_p.ui64_array[15 + (i - 1) * 2];
                    n_p.si64_array[16 + i * 2] = n_p.si64_array[16 + (i - 1) * 2];
                }

                // pull
                n_p.ui64_array[15] = neighbor_p.ui64_array[15 + 2 * neighbor_num_keys];
                neighbor_p.ui64_array[15 + 2 * neighbor_num_keys] = 0;
                n_p.ui64_array[16] = k_prime;

                BufferBlockPointer child_bb = buffer_manager->get_block(
                    table_id, n_p.ui64_array[15], 0);
                v.push_back({child_bb, n_bb.page_num});
                
                parent_p.si64_array[16 + 2 * k_prime_index] =
                    neighbor_p.si64_array[16 + 2 * (neighbor_num_keys - 1)];
                    

                n_p.ui32_array[3] += 1;
                neighbor_p.ui32_array[3] -= 1;

            }

            else
            {
                record rec(neighbor_p.get_pos_value<int64_t>(128 + 12 * (neighbor_num_keys - 1)),
                    neighbor_p.get_pos_value<int16_t>(128 + 8 + 12 * (neighbor_num_keys - 1)),
                    neighbor_p.c_array + neighbor_p.get_pos_value<int16_t>(128 + 10 + 12 * (neighbor_num_keys - 1)));

                insert_into_leaf(&n_p, &rec);
                remove_entry_from_node(&neighbor_p, rec.key, 0);
            }
        }

        /* Case: n is the leftmost child.
        * Take a key-pointer pair from the neighbor to the right.
        * Move the neighbor's leftmost key-pointer pair
        * to n's rightmost position.
        */
        else {
            if(n_p.ui32_array[2] == 1)
            {
                record rec(neighbor_p.get_pos_value<int64_t>(128),
                    neighbor_p.get_pos_value<int16_t>(128 + 8),
                    neighbor_p.c_array + neighbor_p.get_pos_value<int16_t>(128 + 10));

                insert_into_leaf(&n_p, &rec);
                remove_entry_from_node(&neighbor_p, rec.key, 0);
            }
            else
            {
                // pull
                n_p.si64_array[16 + n_num_keys * 2] = k_prime;
                n_p.ui64_array[15 + (n_num_keys + 1) * 2] = neighbor_p.ui64_array[15];

                BufferBlockPointer child_bb = buffer_manager->get_block(
                    table_id, neighbor_p.ui64_array[15], 0);
                v.push_back({child_bb, n_bb.page_num});
                
                parent_p.ui64_array[16 + k_prime_index * 2]
                    = neighbor_p.si64_array[16];

                for(i = 0; i < neighbor_num_keys - 1; i++)
                {
                    neighbor_p.si64_array[16 + i * 2] = neighbor_p.si64_array[16 + (i + 1) * 2];
                    neighbor_p.ui64_array[15 + i * 2] = neighbor_p.ui64_array[15 + (i + 1) * 2];
                }
                
                neighbor_p.ui64_array[15 + i * 2] = neighbor_p.ui64_array[15 + (i + 1) * 2];
                neighbor_p.ui64_array[17 + (neighbor_num_keys - 1) * 2] = 0;
                neighbor_p.ui64_array[16 + (neighbor_num_keys - 1) * 2] = 0;

                n_p.ui32_array[3] += 1;
                neighbor_p.ui32_array[3] -= 1;

            }
        }
    }

    buffer_manager->write_page(parent_bb, parent_p);
    buffer_manager->write_page(n_bb, n_p);
    buffer_manager->write_page(neighbor_bb, neighbor_p);
    for(auto &[bbp, val] : v)
    {
        page_t child_p;
        buffer_manager->get_page(bbp, child_p); 
        child_p.ui64_array[0] = val;
        buffer_manager->write_page(bbp, child_p);
    }

    return root;
}


/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */
pagenum_t delete_entry(int64_t table_id, pagenum_t root, pagenum_t n,
    int64_t key, pagenum_t child)
{
    int min_keys;
    pagenum_t neighbor;
    int neighbor_index;
    int k_prime_index;
    int64_t k_prime;
    int capacity;

    page_t n_p, n_clone;
    BufferBlockPointer n_bb = buffer_manager->get_block(table_id, n, 0, &n_p);
    
    n_clone = n_p;

    try
    {
        // Remove key and pointer from node.
        remove_entry_from_node(&n_p, key, child);
        buffer_manager->write_page(n_bb, n_p);

        /* Case:  deletion from the root. 
        */

        if (n == root) 
            return adjust_root(table_id, root, n_bb);


        /* Case:  deletion from a node below the root.
        * (Rest of function body.)
        */

        if(n_p.ui32_array[2] == 0)
        {
            // if this is internal node,

            /* Case:  node stays at or above minimum.
            * (The simple case.)
            */
            if(n_p.ui32_array[3] >= real_order)
            {
                return root;
            }

            /* Case:  node falls below minimum.
            * Either coalescence or redistribution
            * is needed.
            */
            pagenum_t parent = n_p.ui64_array[0];
            page_t parent_p;
            BufferBlockPointer parent_bb = buffer_manager->get_block(
                table_id, parent, 0, &parent_p);

            neighbor_index = get_neighbor_index(&parent_p, n);
            k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
            k_prime = parent_p.ui64_array[16 + k_prime_index * 2];
            neighbor = neighbor_index == -1 ? parent_p.ui64_array[17] : 
                parent_p.ui64_array[15 + 2 * neighbor_index];

            page_t neighbor_p;
            BufferBlockPointer neighbor_bb = buffer_manager->get_block(
                table_id, neighbor, 0, &neighbor_p);

            /* Coalescence. */
            if(neighbor_p.ui32_array[3] + n_p.ui32_array[3] < 2 * real_order)
            {
                return coalesce_nodes(table_id, root, n_bb, neighbor_bb,
                    neighbor_index, k_prime);
            }
            /* Redistribution. */
            else
            {
                return redistribute_nodes(table_id, root, n_bb, neighbor_bb,
                    neighbor_index, k_prime_index, k_prime);
            }
                
        }
        else
        {

            /* Case:  node stays at or above minimum.
            * (The simple case.)
            */

            if (n_p.ui64_array[14] < 2500)
            {
                return root;
            }

            /* Case:  node falls below minimum.
            * Either coalescence or redistribution
            * is needed.
            */

            /* Find the appropriate neighbor node with which
            * to coalesce.
            * Also find the key (k_prime) in the parent
            * between the pointer to node n and the pointer
            * to the neighbor.
            */

            pagenum_t parent = n_p.ui64_array[0];
            page_t parent_p;
            BufferBlockPointer parent_bb = buffer_manager->get_block(
                table_id, parent, 0, &parent_p);

            neighbor_index = get_neighbor_index(&parent_p, n);
            k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
            k_prime = parent_p.ui64_array[16 + k_prime_index * 2];
            neighbor = neighbor_index == -1 ? parent_p.ui64_array[17] : 
                parent_p.ui64_array[15 + 2 * neighbor_index];
            
            page_t neighbor_p;
            BufferBlockPointer neighbor_bb = buffer_manager->get_block(
                table_id, neighbor, 0, &neighbor_p);
            

            /* Coalescence. */
            if (neighbor_p.ui64_array[14] >= PAGE_SIZE - 128 - n_p.ui64_array[14])
                return coalesce_nodes(table_id, root, n_bb, neighbor_bb,
                    neighbor_index, k_prime);

            /* Redistribution. */
            else  return redistribute_nodes(table_id, root, n_bb, neighbor_bb,
                    neighbor_index, k_prime_index, k_prime);
        }
    }
    catch(const NoSpaceException& e)
    {
        buffer_manager->write_page(n_bb, n_clone);
        throw e;
    }
}
