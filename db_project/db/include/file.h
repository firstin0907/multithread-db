#ifndef DB_FILE_H_
#define DB_FILE_H_

#include <stdint.h>
#include <map>

// These definitions are not requirements.
// You may build your own way to handle the constants.
#define INITIAL_DB_FILE_SIZE (10 * 1024 * 1024)  // 10 MiB
#define PAGE_SIZE (4 * 1024)                     // 4 KiB

typedef uint64_t pagenum_t;

enum PAGE_TYPE
{
	DEFAULT_PAGE = 0, HEADER_PAGE = 1, FREE_PAGE = 2, LEAF_PAGE = 3, INTERNAL_PAGE = 4
};

struct page_t {

  // in-memory page structure
	union
	{
		uint64_t	ui64_array[PAGE_SIZE / sizeof(uint64_t)];
		int64_t		si64_array[PAGE_SIZE / sizeof(int64_t)];
		uint32_t	ui32_array[PAGE_SIZE / sizeof(uint32_t)];
		int32_t		si32_array[PAGE_SIZE / sizeof(int32_t)];
		uint16_t	ui16_array[PAGE_SIZE / sizeof(uint16_t)];
		int16_t		si16_array[PAGE_SIZE / sizeof(int16_t)];
		
		char		c_array[PAGE_SIZE];
	};

	page_t(PAGE_TYPE type = DEFAULT_PAGE);
	
	void print_page(PAGE_TYPE type);

	template<typename T>
	T& get_pos_value(int offset)
	{
		return *(reinterpret_cast<T*>(c_array + offset));
	};

	void clear();
};

extern std::map<int64_t, FILE*> Table_files;

// Open existing table file or create one if it doesn't exist
int64_t file_open_table_file(const char* pathname);

// Allocate an on-disk page from the free page list
uint64_t file_alloc_page(int64_t table_id);

// Free an on-disk page to the free page list
void file_free_page(int64_t table_id, pagenum_t page_number);

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int64_t table_id, pagenum_t page_number, struct page_t* dest);

// Write an in-memory page(src) to the on-disk page
void file_write_page(int64_t table_id, pagenum_t page_number, const struct page_t* src);

// Close the database file
void file_close_table_files();

#endif  // DB_FILE_H_
