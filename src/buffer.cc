#include "global.h"
#include "file.h"
#include "table.h"
#include "buffer.h"
#include "index.h"

Frame::Frame(){
    this->page = nullptr;
    this->is_dirty = 0;
    this->is_pinned = 0;
    this->in_use = 0;
    this->ref = 0;
}

Frame::~Frame(){
    if(this->page){
        if(this->is_dirty){
            File* file = table->get_file(this->table_id);
            file->write_page(this->page_no, this->page);
        }
        delete page;
    }
}

Buffer::Buffer(int size){
    this->size = size;
    this->now_idx = 0;
    this->frames = new Frame[size];
}

Buffer::~Buffer(){
    delete[] frames;
}

void Buffer::pin_page(int table_id, pagenum_t page_no){
    int location;
    if(this->hashtable.count({table_id, page_no})){
        location = this->hashtable[{table_id, page_no}];
        this->frames[location].is_pinned++;
        return;
    }
    while(true){
        this->now_idx++;
        this->now_idx %= this->size;
        Frame& frame = this->frames[this->now_idx];
        if(frame.in_use == 0){
            File* file = table->get_file(table_id);
            file->read_page(page_no, frame.page);
            frame.in_use = 1;
            frame.is_dirty = 0;
            frame.is_pinned++;
            frame.page_no = page_no;
            frame.table_id = table_id;
            this->hashtable[{table_id, page_no}] = now_idx;
            return;
        }else if(frame.is_pinned == 0){
            File* file;
            if(frame.is_dirty){
                file = table->get_file(frame.table_id);
                file->write_page(frame.page_no, frame.page);
            }
            delete frame.page;
            file = table->get_file(table_id);
            frame.page = allocate_page(file->get_header()->get_type());
            file->read_page(page_no, frame.page);
            frame.in_use = 1;
            frame.is_dirty = 0;
            frame.is_pinned++;
            frame.page_no = page_no;
            frame.table_id = table_id;
            this->hashtable[{table_id, page_no}] = now_idx;
            return;
        }
    }
}

void Buffer::unpin_page(int table_id, pagenum_t page_no){
    int location;
    if(this->hashtable.count({table_id, page_no}) == 0){
        //Error!
        //unpin page which is not on buffer pool....
        return;
    }
    location = this->hashtable[{table_id, page_no}];  
    if(this->frames[location].is_pinned==0){
        //Error!
        //unpin when there are no pins....
        return;
    }  
    this->frames[location].is_pinned--; 
}

basic_page* Buffer::get_page(int table_id, pagenum_t page_no){
    int location = this->hashtable[{table_id, page_no}];    
    return this->frames[location].page;
}

void Buffer::free_page(int table_id, pagenum_t page_no){
    int location = this->hashtable[{table_id, page_no}];    
    File* file = table->get_file(table_id);
    
    file->free_page(page_no, this->frames[location].page);
}

pagenum_t Buffer::alloc_page(int table_id){
    File* file = table->get_file(table_id);
    return file->alloc_page();
}