#include "../include/buffer.h"

#include <pthread.h>

#include "../include/file.h"

BufferManager* buffer_manager = nullptr;

pthread_mutex_t buffer_manager_latch = PTHREAD_MUTEX_INITIALIZER;

BufferBlockPointer::BufferBlockPointer(BufferManager* from, int64_t table_id,
    pagenum_t page_num)
: table_id(table_id), page_num(page_num), valid(1), from(from)
{
    
}

BufferBlockPointer::BufferBlockPointer(const BufferBlockPointer& other)
{  
    from = other.from;

    valid = other.valid;
    table_id = other.table_id;
    page_num = other.page_num;

    if(valid && from) from->pin_page(table_id, page_num);
}

BufferBlockPointer::BufferBlockPointer(BufferBlockPointer&& other)
{
    from = other.from;

    valid = other.valid;
    table_id = other.table_id;
    page_num = other.page_num;

    other.from = nullptr;
    other.table_id = -1;
    other.page_num = 0;
    other.valid = 0;
}

BufferBlockPointer& BufferBlockPointer::operator=(const BufferBlockPointer& other)
{    
    if(valid && from) from->unpin_page(table_id, page_num);

    this->from = other.from;
    
    valid = other.valid;
    table_id = other.table_id;
    page_num = other.page_num;

    if(valid && from) from->pin_page(table_id, page_num);

    return *this;
}

BufferBlockPointer& BufferBlockPointer::operator=(BufferBlockPointer&& other)
{
    if(valid && from) from->unpin_page(table_id, page_num);
    
    this->from = other.from;

    valid = other.valid;
    table_id = other.table_id;
    page_num = other.page_num;

    other.from = nullptr;
    other.valid = false;
    other.page_num = -1;

    return *this;
}


BufferBlockPointer::~BufferBlockPointer()
{
    if(valid && from) from->unpin_page(table_id, page_num);
}

BufferManager::BufferManager(int num_buf) : buffer_list_capacity(num_buf)
{
    buffer_list_size = 0;
    buffer_list_head = nullptr;
    calling_count = 0;
}


void BufferManager::set_delete_waited(BufferBlockPointer bbp)
{
    pthread_mutex_lock(&buffer_manager_latch);

    BufferBlock* block = get_block_pointer(bbp.table_id, bbp.page_num);
    block->is_delete_waited = true;

    pthread_mutex_unlock(&buffer_manager_latch);
}

BufferBlockPointer BufferManager::get_block(int64_t table_id,
    pagenum_t page_num, int trx_id, page_t* content)
{
    pthread_mutex_lock(&buffer_manager_latch);

    BufferBlock* it = buffer_list_head;
    BufferBlock* victim = nullptr;

    while(it != nullptr)
    {
        if(table_id == it->table_id && page_num == it->page_num)
        {
            if(victim != nullptr)
            {
                // we don't need to use victim
                victim->is_pinned--;
                pthread_mutex_unlock(&victim->mutex);
                pthread_cond_signal(&victim->cond);
            }

            if(pthread_mutex_trylock(&it->mutex) != 0)
            {
                if(it->using_trx_id == trx_id)
                {
                    if(content != nullptr) *content = it->frame;
                    
                    BufferBlockPointer bb(this, table_id, page_num);
                    it->is_pinned++;
                    pthread_mutex_unlock(&buffer_manager_latch);
                    return bb;
                }
                else
                {
                    pthread_cond_wait(&it->cond, &buffer_manager_latch);
                    pthread_mutex_unlock(&buffer_manager_latch);
                    return get_block(table_id, page_num, trx_id, content);
                }
            }
            else
            {
                if(content != nullptr) *content = it->frame;
                it->is_pinned++;
                it->using_trx_id = trx_id;
            
                BufferBlockPointer bb(this, table_id, page_num);

                pthread_mutex_unlock(&buffer_manager_latch);
                return bb;
            }
            
        }
        else
        {
            if(pthread_mutex_trylock(&it->mutex) == 0)
            {
                it->is_pinned++;
                it->using_trx_id = trx_id;
                    
                if(victim == nullptr)
                {
                    victim = it;
                }
                else if(victim->last_used < it->last_used)
                {
                    victim->is_pinned--;
                    pthread_mutex_unlock(&victim->mutex);
                    pthread_cond_signal(&victim->cond);

                    victim = it;
                }
                else
                {
                    it->is_pinned--;
                    pthread_mutex_unlock(&it->mutex);
                    pthread_cond_signal(&it->cond);
                }
            }
        }
        
        // move it to next of it
        it = it->list_next;
    }

    // case: there is no requested page in buffer
    BufferBlock* new_page = nullptr;

    // if there is no empty space, but there is at least one unpinned page
    if(buffer_list_size == buffer_list_capacity && victim != nullptr)
    {
        // write victim page if dirty, and reuse victim for requested page
        if(victim->is_dirty == true)
        {
            if(victim->table_id != -1)
            {
                file_write_page(victim->table_id, victim->page_num,
                    &(victim->frame));
                victim->is_dirty = false;
            }
        }
        new_page = victim;
    }
    // if there is some empty space on list,
    else if(buffer_list_size < buffer_list_capacity)
    {
        new_page = new BufferBlock;
        new_page->mutex = PTHREAD_MUTEX_INITIALIZER;
        new_page->cond = PTHREAD_COND_INITIALIZER;

        // put new page into the front of list
        if(buffer_list_head != nullptr) buffer_list_head->list_prev = new_page;
        new_page->list_next = buffer_list_head;
        buffer_list_head = new_page;
        ++buffer_list_size;

        pthread_mutex_lock(&new_page->mutex);
    }

    // if we could make new page,
    if(new_page != nullptr)
    {
        file_read_page(table_id, page_num, &(new_page->frame));

        new_page->table_id = table_id;
        new_page->page_num = page_num;
        new_page->using_trx_id = trx_id;
        new_page->is_dirty = false;

        // set pin count as 1
        new_page->is_pinned = 1;
        new_page->last_used = calling_count;
        new_page->is_delete_waited = false;
        
        if(content != nullptr) *content = new_page->frame;

        // successfully made new page!        
        BufferBlockPointer bb(this, table_id, page_num);

        pthread_mutex_unlock(&buffer_manager_latch);
        return bb;
    }


    pthread_mutex_unlock(&buffer_manager_latch);

    // case : there is no space, and no unpinned pages, it fails.
    throw NoSpaceException();
}

BufferBlockPointer BufferManager::get_new_block(int64_t table_id, int trx_id, PAGE_TYPE page_type)
{
    /*
    pthread_mutex_lock(&buffer_manager_latch);
    pagenum_t page_num = file_alloc_page(table_id);
    pthread_mutex_unlock(&buffer_manager_latch);
    try
    {
        BufferBlockPointer bbp = get_block(table_id, page_num, trx_id, nullptr);
        return bbp;
    }
    catch(const NoSpaceException& e)
    {
        pthread_mutex_lock(&buffer_manager_latch);
        file_free_page(table_id, page_num);
        pthread_mutex_unlock(&buffer_manager_latch);
        throw;
    }
    */
    BufferBlock* new_page;
    
    pthread_mutex_lock(&buffer_manager_latch);

    if(buffer_list_size < buffer_list_capacity)
    {
        new_page = new BufferBlock;
        new_page->mutex = PTHREAD_MUTEX_INITIALIZER;
        new_page->cond = PTHREAD_COND_INITIALIZER;
        pthread_mutex_lock(&new_page->mutex);

        // put new page into the front of list
        if(buffer_list_head != nullptr) buffer_list_head->list_prev = new_page;
        new_page->list_next = buffer_list_head;
        buffer_list_head = new_page;

        ++buffer_list_size;
    }
    else
    {
        BufferBlock* it = buffer_list_head;
        BufferBlock* victim = nullptr;
        while(it != nullptr)
        {
            if(pthread_mutex_trylock(&it->mutex) == 0)
            {
                it->is_pinned++;
                it->using_trx_id = trx_id;
                    
                if(victim == nullptr)
                {
                    victim = it;
                }
                else if(victim->last_used < it->last_used)
                {
                    victim->is_pinned--;
                    pthread_mutex_unlock(&victim->mutex);
                    pthread_cond_signal(&victim->cond);

                    victim = it;
                }
                else
                {
                    it->is_pinned--;
                    pthread_mutex_unlock(&it->mutex);
                    pthread_cond_signal(&it->cond);
                }
            }
            // move it to next of it
            it = it->list_next;
        }

        if(victim == nullptr)
        {
            pthread_mutex_unlock(&buffer_manager_latch);
            throw NoSpaceException();
        }

        // write victim page if dirty, and reuse victim for requested page
        if(victim->is_dirty == true)
        {
            file_write_page(victim->table_id, victim->page_num,
                &(victim->frame));
            victim->is_dirty = false;
        }
        new_page = victim;
    }

    // if there is some place for new page in buffer.
    new_page->frame = page_t(page_type);

    new_page->table_id = table_id;
    new_page->page_num = file_alloc_page(table_id);
    new_page->is_dirty = true;
    // set pin count as 0, it will be increased soon
    // at the BufferBlockPointer constructure
    new_page->is_pinned = 1;
    new_page->last_used = calling_count;
    new_page->is_delete_waited = false;
    new_page->using_trx_id = trx_id;


    BufferBlockPointer bb(this, new_page->table_id, new_page->page_num);
    pthread_mutex_unlock(&buffer_manager_latch);
    return bb;
}

int64_t BufferManager::open_table(const char* pathname)
{
    return file_open_table_file(pathname);
}

void BufferManager::get_page(BufferBlockPointer bbp, page_t& page)
{
    pthread_mutex_lock(&buffer_manager_latch);
    
    BufferBlock* block = get_block_pointer(bbp.table_id, bbp.page_num);
    page = block->frame;

    pthread_mutex_unlock(&buffer_manager_latch);
}

void BufferManager::write_page(BufferBlockPointer bbp, const page_t& content)
{
    pthread_mutex_lock(&buffer_manager_latch);
    BufferBlock* block = get_block_pointer(bbp.table_id, bbp.page_num);
    
    block->frame = content;
    block->is_dirty = true;
    pthread_mutex_unlock(&buffer_manager_latch);
}

void BufferManager::free_page(int64_t table_id, pagenum_t page_num)
{
    BufferBlock* block = get_block_pointer(table_id, page_num);

    file_free_page(block->table_id, block->page_num);
    block->table_id = -1;

    // replace it next time
    block->last_used = 0;
}

void BufferManager::pin_page(int64_t table_id, pagenum_t page_num)
{
    pthread_mutex_lock(&buffer_manager_latch);
    BufferBlock* block = get_block_pointer(table_id, page_num);
    
    // can assumpt that this trx already holds a mutex,
    // so call lock function is not needed. 
    block->is_pinned++;

    pthread_mutex_unlock(&buffer_manager_latch);
}

void BufferManager::unpin_page(int64_t table_id, pagenum_t page_num)
{
    pthread_mutex_lock(&buffer_manager_latch);
    BufferBlock* block = get_block_pointer(table_id, page_num);

    block->last_used = ++calling_count;
    block->is_pinned--;

    // if there is no pin, unlock page latch
    if(block->is_pinned <= 0)
    {
        if(block->is_delete_waited) free_page(table_id, page_num);
        pthread_mutex_unlock(&block->mutex);
        pthread_cond_signal(&block->cond);
    }

    pthread_mutex_unlock(&buffer_manager_latch);
}

void BufferManager::close_tables()
{
    file_close_table_files();
}

void BufferManager::clear_pages()
{
    BufferBlock* curr = buffer_list_head;

    while(curr != nullptr)
    {
        BufferBlock* nxt = curr->list_next;

        if(curr->is_dirty)
        {
            file_write_page(curr->table_id, curr->page_num, &(curr->frame));
        }
        delete curr;
        
        curr = nxt;
    }
    
    buffer_list_head = nullptr;
}



BufferBlock* BufferManager::get_block_pointer(
    int64_t table_id, pagenum_t page_num)
{
    
    BufferBlock* it = buffer_list_head;

    while(it != nullptr)
    {
         // if requested page was founded,
        if(table_id == it->table_id && page_num == it->page_num)
        {
            // return the page;
            return it;
        }

        // move it to next of it
        it = it->list_next;
    }
    
    return nullptr;
}


BufferManager::~BufferManager()
{
    clear_pages();
}