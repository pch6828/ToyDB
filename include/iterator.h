#ifndef __ITERATOR_H__
#define __ITERATOR_H__

#include "global.h"
#include "index.h"

class iterator{
protected:
    int table_id;
    pagenum_t page_no;
    int idx;
public:
    virtual ~iterator(){};
    virtual void set_iter_begin() = 0;
    virtual void set_iter_from(int64_t key) = 0;
    virtual Record* next() = 0;
};

class bplustree_iterator : public iterator{
public:
    bplustree_iterator(int table_id);
    virtual ~bplustree_iterator(){};
    virtual void set_iter_begin();
    virtual void set_iter_from(int64_t key);
    virtual Record* next();
};

class skiplist_iterator : public iterator{
public:
    skiplist_iterator(int table_id);
    virtual ~skiplist_iterator(){};
    virtual void set_iter_begin();
    virtual void set_iter_from(int64_t key);
    virtual Record* next();
};

#endif