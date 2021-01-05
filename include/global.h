#ifndef __GLOBAL_H__
#define __GLOBAL_H__

#include <cstdint>

#define BPLUSTREE 0
#define RADIXTREE 1
#define SKIPLIST  2

#define PAGE_SIZE 4096
#define HEADER_PAGE_NO 0

typedef uint64_t pagenum_t;

class Record{
private:
    int64_t key;
    char value[120];
public:
    Record();
    Record(int64_t key, char* value);
    ~Record();
    
    //for update
    void set_value(char* value);
    int64_t get_key();
    char* get_value();
};

class basic_page{
protected:
    pagenum_t parent_no;
    int flag;
    int num_keys;
    char reserved[92];
    //4 byte for vptr
public:
    basic_page();
    ~basic_page();
    void set_parent(pagenum_t parent);
    pagenum_t get_parent();
    void clean_page();
    virtual bool insert(int table_id, int64_t key, char* value) = 0;
    virtual bool erase(int table_id, int64_t key) = 0;
    virtual char* find(int table_id, int64_t key) = 0;
    virtual void print() = 0;
};

class HeaderPage{
private:
    pagenum_t free_page_no;
    pagenum_t root_page_no;
    uint64_t number_of_pages;
    int index_type;
    char reserved[4068];
public:
    HeaderPage();
    HeaderPage(int index_type);
    ~HeaderPage(); 
    int get_type();
    pagenum_t get_free_page();
    pagenum_t get_root_page();
    uint64_t get_size();
    void set_root_page(pagenum_t root);
    void set_free_page(pagenum_t free);
    void increase_page();
};

#endif