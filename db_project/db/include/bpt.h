
#pragma once

// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <vector>


#ifdef WINDOWS
#define bool char
#define false 0
#define true 1
#endif

// Default order is 4.
#define DEFAULT_ORDER 4

// Constants for printing part or all of the GPL license.
#define LICENSE_FILE "LICENSE.txt"
#define LICENSE_WARRANTEE 0
#define LICENSE_WARRANTEE_START 592
#define LICENSE_WARRANTEE_END 624
#define LICENSE_CONDITIONS 1
#define LICENSE_CONDITIONS_START 70
#define LICENSE_CONDITIONS_END 625

// TYPES.

/* Type representing the record
 * to which a given key refers.
 * In a real B+ tree system, the
 * record would hold data (in a database)
 * or a file (in an operating system)
 * or some other information.
 * Users can rewrite this part of the code
 * to change the type and content
 * of the value field.
 */

struct page_t;

typedef struct record {
    int64_t     key;
    uint16_t    size;
    const char* content;
    
    record(int64_t key, uint16_t size, const char* content);
} record;

/* Type representing a node in the B+ tree.
 * This type is general enough to serve for both
 * the leaf and the internal node.
 * The heart of the node is the array
 * of keys and the array of corresponding
 * pointers.  The relation between keys
 * and pointers differs between leaves and
 * internal nodes.  In a leaf, the index
 * of each key equals the index of its corresponding
 * pointer, with a maximum of order - 1 key-pointer
 * pairs.  The last pointer points to the
 * leaf to the right (or NULL in the case
 * of the rightmost leaf).
 * In an internal node, the first pointer
 * refers to lower nodes with keys less than
 * the smallest key in the keys array.  Then,
 * with indices i starting at 0, the pointer
 * at i + 1 points to the subtree with keys
 * greater than or equal to the key in this
 * node at index i.
 * The num_keys field is used to keep
 * track of the number of valid keys.
 * In an internal node, the number of valid
 * pointers is always num_keys + 1.
 * In a leaf, the number of valid pointers
 * to data is always num_keys.  The
 * last leaf pointer points to the next leaf.
 */
typedef struct node {
    void ** pointers;
    int * keys;
    struct node * parent;
    bool is_leaf;
    int num_keys;
    struct node * next; // Used for queue.
} node;

typedef uint64_t pagenum_t;

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
extern int order;

/* The queue is used to print the tree in
 * level order, starting from the root
 * printing each entire rank on a separate
 * line, finishing with the leaves.
 */
extern node * queue;

/* The user can toggle on and off the "verbose"
 * property, which causes the pointer addresses
 * to be printed out in hexadecimal notation
 * next to their corresponding keys.
 */
extern bool verbose_output;


// FUNCTION PROTOTYPES.

// Output and utility.

void license_notice( void );
void print_license( int licence_part );
void find_and_print(node * root, int key, bool verbose); 
void find_and_print_range(node * root, int range1, int range2, bool verbose); 
int find_range(int64_t table_id, pagenum_t root, int64_t key_start, int64_t key_end,
    std::vector<int64_t> * keys, std::vector<char*> * values, std::vector<uint16_t> * val_sizes);

record* find_record(int64_t table_id,
    pagenum_t root, int64_t key, int trx_id);
struct BufferBlockPointer find_leaf(int64_t table_id,
    pagenum_t root, int64_t key, int trx_id);


int cut( int length );

// Insertion.
bool insert_into_leaf(page_t* leaf, const record* src);
pagenum_t insert_into_leaf_after_splitting(
    int64_t table_id, pagenum_t root, pagenum_t leaf, const record* src);
bool insert_into_node(int64_t table_id, pagenum_t n, int64_t key, pagenum_t right);
pagenum_t insert_into_node_after_splitting(int64_t table_id, pagenum_t root,
    pagenum_t old_node, int64_t key, pagenum_t right);
pagenum_t insert_into_parent(int64_t table_id, pagenum_t root, pagenum_t parent,
        pagenum_t left, int64_t key, pagenum_t right);
pagenum_t insert_into_new_root(int64_t table_id, pagenum_t left,
    int64_t key, pagenum_t right);
pagenum_t start_new_tree(int64_t table_id, const record* src);
pagenum_t insert(int64_t table_id, pagenum_t root, const record* src);

// Deletion.


int get_neighbor_index(const page_t* parent_p, pagenum_t n);
void remove_entry_from_node(page_t* n_p, int64_t key, pagenum_t child);

pagenum_t adjust_root(int64_t table_id, pagenum_t root, page_t* root_p);
pagenum_t coalesce_nodes(int64_t table_id, pagenum_t root, BufferBlockPointer n_bb,
    BufferBlockPointer neighbor_bb, int neighbor_index, int64_t k_prime);
pagenum_t redistribute_nodes(int64_t table_id, pagenum_t root,
    BufferBlockPointer n_bb, BufferBlockPointer neighbor_bb,
    int neighbor_index, int k_prime_index, int k_prime);
pagenum_t delete_entry(int64_t table_id, pagenum_t root,
    pagenum_t n, int64_t key, pagenum_t child);

