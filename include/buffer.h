#ifndef __BUFFER_H__
#define __BUFFER_H__

#include <unordered_map>
#include <functional>
#include "global.h"
#include "file.h"

class Hash{
public:
    size_t operator()(const std::pair<int, pagenum_t>& p)const{
        std::hash<int>hash1;
        std::hash<pagenum_t>hash2;
        std::hash<uint64_t>hash3;

        ssize_t a = hash1(p.first);
        ssize_t b = hash2(p.second);

        return hash3(a<<32+b);
    }
};

class Frame{
private:
    basic_page* page;
    int table_id;
    pagenum_t page_no;
    int is_dirty;
    int is_pinned;
    int in_use;
    int ref;
public:
    Frame();
    ~Frame();
    friend class Buffer;
};

class Buffer{
private:
    std::unordered_map<std::pair<int, pagenum_t>, int, Hash>hashtable;
    Frame* frames;
    int size;
    int now_idx;
public:
    Buffer(int size);
    ~Buffer();

    void pin_page(int table_id, pagenum_t page_no);
    void unpin_page(int table_id, pagenum_t page_no);
    basic_page* get_page(int table_id, pagenum_t page_no);
    void free_page(int table_id, pagenum_t page_no);
    pagenum_t alloc_page(int table_id);
};

extern Buffer* buffer;
#endif