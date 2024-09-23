#include "../db/include/file.h"

#include <gtest/gtest.h>
#include <cstdlib>
#include <string>

/*******************************************************************************
 * The test structures stated here were written to give you and idea of what a
 * test should contain and look like. Feel free to change the code and add new
 * tests of your own. The more concrete your tests are, the easier it'd be to
 * detect bugs in the future projects.
 ******************************************************************************/

/*
 * Tests file open/close APIs.
 * 1. Open a file and check the descriptor
 * 2. Check if the file's initial size is 10 MiB
 */
TEST(FileInitTest, HandlesInitialization) {
  int fd;                                 // file descriptor
  std::string pathname = "init_test.db";  // customize it to your test file

  // Open a database file
  fd = file_open_table_file(pathname.c_str());

  // Check if the file is opened
  ASSERT_TRUE(fd >= 0);  // change the condition to your design's behavior

  // Check the size of the initial file  
  page_t header_page;
  file_read_page(fd, 0, &header_page);	// read header page from disk.
  
  int num_pages = header_page.ui64_array[2]; // get number of pages.
  
  EXPECT_EQ(num_pages, INITIAL_DB_FILE_SIZE / PAGE_SIZE)
      << "The initial number of pages does not match the requirement: "
      << num_pages;

  // Close all database files
  file_close_table_files();

  // Remove the db file
  int is_removed = remove(pathname.c_str());

  ASSERT_EQ(is_removed, /* 0 for success */ 0);
}

/*
 * TestFixture for page allocation/deallocation tests
 */
class FileTest : public ::testing::Test {
 protected:
  /*
   * NOTE: You can also use constructor/destructor instead of SetUp() and
   * TearDown(). The official document says that the former is actually
   * perferred due to some reasons. Checkout the document for the difference
   */
  FileTest() {
	  pathname = "test_test_db.db";
	  fd = file_open_table_file(pathname.c_str());
  }

  ~FileTest() {
    if (fd >= 0) {
      file_close_table_files();
    }
  }

  int fd;                // file descriptor
  std::string pathname;  // path for the file
};

/*
 * Tests page allocation and free
 * 1. Allocate 2 pages and free one of them, traverse the free page list
 *    and check the existence/absence of the freed/allocated page
 */
TEST_F(FileTest, HandlesPageAllocation) {
  pagenum_t allocated_page, freed_page;

  // Allocate the pages
  allocated_page = file_alloc_page(fd);
  freed_page = file_alloc_page(fd);
  
  // Free one page
  file_free_page(fd, freed_page);

  // Traverse the free page list and check the existence of the freed/allocated
  // pages. You might need to open a few APIs soley for testing.
  
  bool existence_of_ap = false;
  bool existence_of_fp = false;

  page_t content;
  file_read_page(fd, 0, &content);

  pagenum_t it = content.ui64_array[1];
  while(it)
  {
	  if(it == allocated_page) existence_of_ap = true;
	  if(it == freed_page) existence_of_fp = true;
	  
	  file_read_page(fd, it, &content);
	  it = content.ui64_array[0];

  }

  EXPECT_TRUE(existence_of_fp) << "Couldn't find free page on list!";
  EXPECT_FALSE(existence_of_ap) << "Allocated page has been detected!";
  
}

/*
 * Tests page read/write operations
 * 1. Write/Read a page with some random content and check if the data matches
 */
TEST_F(FileTest, CheckReadWriteOperation) {
	pagenum_t pagenum = file_alloc_page(fd);
	page_t src, dest;

	for(int i = 0; i < PAGE_SIZE; i++)
	{
		src.c_array[i] = 'a' + rand() % ('z' - 'a');
	}

	file_write_page(fd, pagenum, &src);
	file_read_page(fd, pagenum, &dest);

	for(int i = 0; i < PAGE_SIZE; i++)
	{
		EXPECT_EQ(src.c_array[i], dest.c_array[i])
		       	<< "difference of content between written and read has been detected!";
	}

}


/*
 * Tests page allocation APIs whether it conduct doubling pages.
 * 1. Open a file and check the descriptor
 * 2. Check the number of free pages and all pages.
 * 3. Allocate all free pages and one more page.
 * 4. Check the number of all pages has been doubled.
 */

TEST(FileTest2, CheckExtendReservedPage) {
	
	std::string pathname = "init_test.db";	// db file name
	int fd;                                 // file descriptor
	page_t header_page;
	
	// Open a database file and check if the file is opened.
	fd = file_open_table_file(pathname.c_str());
	
	// Check if the file is opened
	ASSERT_TRUE(fd >= 0);
	// read header page.
	file_read_page(fd, 0, &header_page);

	// get number of all pages from header page.
	uint64_t num_pages_before = header_page.ui64_array[2];

	// allocate all of free pages.
	while(header_page.ui64_array[1])
	{
		file_alloc_page(fd);
		file_read_page(fd, 0, &header_page);
	}
	file_alloc_page(fd);

	// allocate one more page, which should invoke extension.
	file_read_page(fd, 0, &header_page);

	// current page number should be 2 * (page number before allocation).
	EXPECT_EQ(num_pages_before * 2, header_page.ui64_array[2]);
	// Close all database files
	file_close_table_files();

	// Remove the db file
	int is_removed = remove(pathname.c_str());
	ASSERT_EQ(is_removed, 0);
}

