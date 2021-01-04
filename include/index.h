#ifndef __INDEX_H__
#define __INDEX_H__

#include "global.h"

class branch{
private:
    int64_t key;
    pagenum_t page_no;
public:
    branch();
    ~branch();

    void set_key(int64_t key);
    void set_page_no(pagenum_t page_no);
    int64_t get_key();
    pagenum_t get_page_no();
};

class bplustree : public basic_page{
private:
    union{
        pagenum_t leftmost_child;
        pagenum_t right_sibling;
    };
    union{
        Record records[31];
        branch child[248];
    };

public:
    virtual bool insert(int64_t key, char* value){};
    virtual bool erase(int64_t key){};
    virtual char* find(int64_t key){};
};

basic_page* allocate_page(int index_type);

#endif