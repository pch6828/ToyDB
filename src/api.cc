#include <string>
#include <cstdint>

#include "global.h"
#include "file.h"
#include "table.h"
#include "buffer.h"
#include "index.h"
#include "api.h"

int init_db(int buffer_size){
    if(buffer!=nullptr){
        delete buffer;
    }
    buffer = new Buffer(buffer_size);
    if(table!=nullptr){
        delete table;
    }
    table = new Table();
    return 0;
}

int end_db(){
    delete buffer;
    delete table;
    buffer = nullptr;
    table = nullptr;
}

int open(std::string filename, int index_type){
    int table_id = table->open(filename, index_type);
    if(table->get_file(table_id)->get_header()->get_type()!=index_type){
        table->close(table_id);
        return -1;
    }
    return table_id;
}

int close(int table_id){
    table->close(table_id);
}

bool insert(int table_id, int64_t key, char* value){
    bool result;
    File* file = table->get_file(table_id);
    if(!file){
        return false;
    }
    pagenum_t root_no;
    if((root_no = file->get_header()->get_root_page())==0){
        file->get_header()->set_root_page(root_no = file->alloc_page());
    }

    buffer->pin_page(table_id, root_no);
    bplustree* root = (bplustree*)(buffer->get_page(table_id, root_no));
    result = root->insert(table_id, key, value);
    buffer->unpin_page(table_id, root_no);
    return result;
}

char* find(int table_id, int64_t key){
    char* result;
    File* file = table->get_file(table_id);
    if(!file){
        return nullptr;
    }
    pagenum_t root_no;
    if((root_no = file->get_header()->get_root_page())==0){
        file->get_header()->set_root_page(root_no = file->alloc_page());
    }

    buffer->pin_page(table_id, root_no);
    bplustree* root = (bplustree*)(buffer->get_page(table_id, root_no));
    result = root->find(table_id, key);
    buffer->unpin_page(table_id, root_no);
    return result;
}

bool erase(int table_id, int64_t key){    
    bool result;
    File* file = table->get_file(table_id);
    if(!file){
        return false;
    }
    pagenum_t root_no;
    if((root_no = file->get_header()->get_root_page())==0){
        file->get_header()->set_root_page(root_no = file->alloc_page());
    }

    buffer->pin_page(table_id, root_no);
    bplustree* root = (bplustree*)(buffer->get_page(table_id, root_no));
    result = root->erase(table_id, key);
    buffer->unpin_page(table_id, root_no);
    return result; 
}

void print(int table_id){
    File* file = table->get_file(table_id);
    if(!file){
        return;
    }
    pagenum_t root_no;
    if((root_no = file->get_header()->get_root_page())==0){
        file->get_header()->set_root_page(root_no = file->alloc_page());
    }

    buffer->pin_page(table_id, root_no);
    bplustree* root = (bplustree*)(buffer->get_page(table_id, root_no));
    root->print(table_id);
    buffer->unpin_page(table_id, root_no);
}

