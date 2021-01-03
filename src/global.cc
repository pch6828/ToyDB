#include <cstring>
#include "global.h"

Record::Record(){
    this->key = 0;
    this->value[0] = '\0';
}

Record::Record(int64_t key, char* value){
    this->key = key;
    memcpy(this->value, value, 120);
}

Record::~Record(){
    //Do Nothing
}

void Record::set_value(char* value){
    memcpy(this->value, value, 120);
}

int64_t Record::get_key(){
    return this->key;
}

char* Record::get_value(){
    char* result = new char[120];
    memcpy(result, this->value, 120);
    return result;
}

void basic_page::set_parent(pagenum_t parent){
    this->parent_no = parent;
}

pagenum_t basic_page::get_parent(){
    return this->parent_no;
}
void basic_page::clean_page(){
    parent_no = 0;
    flag = 0;
    num_keys = 0;
}

HeaderPage::HeaderPage(){
    this->free_page_no = 0;
    this->root_page_no = 0;
    this->number_of_pages = 1;
    this->index_type = BPLUSTREE;
}

HeaderPage::HeaderPage(int index_type){
    this->free_page_no = 0;
    this->root_page_no = 0;
    this->number_of_pages = 1;
    this->index_type = index_type;
}

HeaderPage::~HeaderPage(){    
    //Do Nothing
}

int HeaderPage::get_type(){
    return this->index_type;
}

pagenum_t HeaderPage::get_free_page(){
    return this->free_page_no;
}

pagenum_t HeaderPage::get_root_page(){
    return this->root_page_no;
}

uint64_t HeaderPage::get_size(){
    return this->number_of_pages;
}

void HeaderPage::set_root_page(pagenum_t root){
    this->root_page_no = root;
}

void HeaderPage::set_free_page(pagenum_t free){
    this->free_page_no = free;
}

void HeaderPage::increase_page(){
    this->number_of_pages++;
}