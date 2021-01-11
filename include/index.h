#ifndef __INDEX_H__
#define __INDEX_H__

#include "global.h"

class Branch{
private:
    int64_t key;
    pagenum_t page_no;
public:
    void set_key(int64_t key);
    void set_page_no(pagenum_t page_no);
    int64_t get_key();
    pagenum_t get_page_no();
};

class bplustree : public basic_page{
private:
    //flag is indicating whether this node is leaf or not
    union{
        pagenum_t leftmost_child;
        pagenum_t right_sibling;
    };
    union{
        Record records[31];
        Branch child[248];
    };
    int find_idx(pagenum_t page_no);
    int height(int table_id);

    pagenum_t find_leaf(int table_id, int64_t key, pagenum_t now);//
    
    void add_record(Record& record);
    void add_branch(int table_id, pagenum_t now, Branch& branch);
    void insert_branch(int table_id, pagenum_t now, Branch& branch);
    void split_leaf(int table_id, Record& record);
    void split_internal(int table_id, pagenum_t left_no, Branch& branch);
    void init_root(int table_id, pagenum_t now, pagenum_t leftmost, Branch& branch);
    
    void remove_record(int64_t key);
    void remove_branch(int table_id, int64_t key);
    void erase_entry(int table_id, pagenum_t now, int64_t key);
    void adjust_root(int table_id, pagenum_t now);
    void redistribute_node(int table_id, pagenum_t left_no, pagenum_t right_no, int idx);
    void merge_node(int table_id, pagenum_t parent_no, pagenum_t left_no, pagenum_t right_no, int idx);
public:
    bplustree();
    bool insert(int table_id, int64_t key, char* value);
    bool erase(int table_id, int64_t key);
    char* find(int table_id, int64_t key);
    void print(int table_id);
};

class skiplist : public basic_page{
private:
    //parent_no is not used in skiplist
    //flag is indicating level of node
    char reserved[8];
    Record records[23];
    Branch prev[16];
    Branch next[16];

    pagenum_t find_node(int table_id, int64_t key, pagenum_t now, int now_lvl);
public:
    skiplist();
    bool insert(int table_id, int64_t key, char* value);
    bool erase(int table_id, int64_t key);
    char* find(int table_id, int64_t key);
    void print(int table_id);
};

basic_page* allocate_page(int index_type);
#endif