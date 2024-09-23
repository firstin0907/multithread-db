#include <map>
#include <memory.h>
#include <unistd.h>
#include <stdexcept>
#include <exception>
#include <iostream>
#include <cstdio>

#include "../include/file.h"

constexpr uint64_t MAGIC_NUMBER = 2022;

std::map<int64_t, FILE*> Table_files;

page_t::page_t(PAGE_TYPE type)
{
	switch(type)
	{
	case HEADER_PAGE:
		break;

	case FREE_PAGE:
		break;

	case LEAF_PAGE:
		clear();
		ui32_array[2] = 1; // is_leaf = true
		ui32_array[3] = 0; // number of keys = 0;
		ui64_array[112 / 8] = PAGE_SIZE - 128; // amount of free space
		break;

	case INTERNAL_PAGE:
		clear();
		ui32_array[2] = 0; // is_leaf = false
		ui32_array[3] = 0; // number of keys = 0;
		break;

	default:
		break;
	}
}

void page_t::print_page(PAGE_TYPE type)
{
	std::cout << "print page ===============================" << std::endl;
	switch(type)
	{
		case LEAF_PAGE: {
			uint32_t num_keys = ui32_array[3];
			for(uint32_t i = 0; i < num_keys; i++)
			{
				uint16_t size = get_pos_value<uint16_t>(128 + 8 + i * 12);
				uint16_t offset = get_pos_value<uint16_t>(128 + 10 + i * 12);
				std::cout << get_pos_value<int64_t>(128 + i * 12) << ", ";
				std::cout << size << ", ";
				std::cout << offset << " : ";

				for(uint16_t i = 0; i < size; i++)
				{
					std::cout << c_array[i + offset];
				}
				std::cout << "\n";
			}}
			break;

		case INTERNAL_PAGE:{
			uint32_t num_keys= ui32_array[3];
			std::cout << "|o" << si64_array[15];
			for(uint32_t i = 0; i < num_keys; i++)
				std::cout << "|" << si64_array[16 + i * 2] << "|o"
				<< si64_array[17 + i * 2];
			break;}
			puts("");
	}
	std::cout << "==========================================" << std::endl;
}

/*
template<typename T>
T& page_t::get_pos_value(int offset)
{
	return &reinterpret_cast<T*>(c_array + offset);
}
*/
void page_t::clear()
{
	memset(&ui32_array, 0, sizeof(ui32_array));
}



// Open existing table file or create one if it doesn't exist
int64_t file_open_table_file(const char* pathname)
{
	page_t header_page, normal_page;

	FILE* db_file = fopen(pathname, "r+");

	if (db_file == nullptr)
	{
		// if file doesn't exist, create one
		db_file = fopen(pathname, "w");
		if (db_file == nullptr) return -1;

		int fd = fileno(db_file);
		Table_files[fd] = db_file;
		
		header_page.clear();
		normal_page.clear();

		header_page.ui64_array[0] = MAGIC_NUMBER;
		header_page.ui64_array[1] = 1;
		header_page.ui64_array[2] = INITIAL_DB_FILE_SIZE / PAGE_SIZE; // number of pages
		header_page.ui64_array[3] = 0; // root page number

		file_write_page(fd, 0, &header_page);
		
		for (int i = 1; i < INITIAL_DB_FILE_SIZE / PAGE_SIZE; i++)
		{
			if (i != INITIAL_DB_FILE_SIZE / PAGE_SIZE - 1)
			{
				normal_page.ui64_array[0] = i + 1;
			}
			else normal_page.ui64_array[0] = 0;

			file_write_page(fd, i, &normal_page);
		}
		fclose(db_file);

		db_file = fopen(pathname, "r+");
		if (db_file == nullptr)
		{
			return -1;
		}
	}

	int fd = fileno(db_file);
	Table_files[fd] = db_file;

	file_read_page(fd, 0, &header_page);
	if (header_page.ui64_array[0] != MAGIC_NUMBER)
	{
		fclose(db_file);
		db_file = nullptr;
		return -1;
	}
	
	return fd;
}

// Allocate an on-disk page from the free page list
uint64_t file_alloc_page(int64_t table_id)
{
	page_t header_page, next_page;
	file_read_page(table_id, 0, &header_page);
	
	if (header_page.ui64_array[1] == 0)
	{
		// doubling
		page_t normal_page;
		pagenum_t page_num = header_page.ui64_array[2];
		for (auto i = page_num; i < page_num * 2; i++)
		{
			if (page_num * 2 - 1 == i) normal_page.ui64_array[0] = 0;
			else normal_page.ui64_array[0] = i + 1;

			file_write_page(table_id, i, &normal_page);
		}

		header_page.ui64_array[1] = page_num; // set next page
		header_page.ui64_array[2] = page_num * 2; // set number of page		
	}
	
	pagenum_t next = header_page.ui64_array[1];
	file_read_page(table_id, next, &next_page);

	pagenum_t next_next = next_page.ui64_array[0];
	header_page.ui64_array[1] = next_next;
	file_write_page(table_id, 0, &header_page);
	
	return next;
}

// Free an on-disk page to the free page list
void file_free_page(int64_t table_id, pagenum_t page_number)
{
	auto file_it = Table_files.find(table_id);
	if (file_it == Table_files.end())
	{
		throw std::out_of_range("Wrong table id!");
	}

	page_t header_page, next_page;
	file_read_page(table_id, 0, &header_page);
	file_read_page(table_id, page_number, &next_page);

	next_page.ui64_array[0] = header_page.ui64_array[1];
	header_page.ui64_array[1] = page_number;

	file_write_page(table_id, 0, &header_page);
	file_write_page(table_id, page_number, &next_page);


}

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int64_t table_id, pagenum_t page_number, struct page_t* dest)
{
	auto file_it = Table_files.find(table_id);
	if (file_it == Table_files.end())
	{
		throw std::out_of_range("Wrong table id!");
	}

	FILE* file = file_it->second;
	fseek(file, page_number * PAGE_SIZE, SEEK_SET);
	fread(dest, sizeof(page_t), 1, file);
}

// Write an in-memory page(src) to the on-disk page
void file_write_page(int64_t table_id, pagenum_t page_number, const struct page_t* src)
{
	auto file_it = Table_files.find(table_id);
	if (file_it == Table_files.end())
	{
		throw std::out_of_range("Wrong table id!");
	}

	FILE* file = file_it->second;
	fseek(file, page_number * PAGE_SIZE, SEEK_SET);
	fwrite(src, sizeof(page_t), 1, file);
	
	// Synchronize the page on disk and the page in memory.
	if(fsync(table_id) != 0)
	{
		throw std::runtime_error("Failed to synchronize disk and memory.");
	}
}

// Close the database file
void file_close_table_files()
{
	// Iterate all (fd, file) pairs and close them.
	for (auto& i : Table_files)
	{
		fclose(i.second);
		i.second = nullptr;
	}
	Table_files.clear();
}

