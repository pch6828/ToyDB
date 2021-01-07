#include "global.h"
#include "index.h"

basic_page* allocate_page(int index_type){
    if(index_type == BPLUSTREE){
        return new bplustree();
    }
    return nullptr;
}

void Branch::set_key(int64_t key){
    this->key = key;
}

void Branch::set_page_no(pagenum_t page_no){
    this->page_no = page_no;
}

int64_t Branch::get_key(){
    return this->key;
}

pagenum_t Branch::get_page_no(){
    return this->page_no;
}