#ifndef __FILE_H__
#define __FILE_H__

#include <string>
#include "global.h"

class File{
private:
    int fd;
    HeaderPage* header;
public:
    File(std::string filename, int index = BPLUSTREE);
    ~File();
    
    void write_page(pagenum_t page_no, basic_page* page);
    void read_page(pagenum_t page_no, basic_page* page);
    pagenum_t alloc_page();
    void free_page(pagenum_t page_no, basic_page* page);

    void write_header();
    void read_header();
};

class Page : public basic_page{
private:
    char reserved[3976];
public:
    virtual bool insert(int64_t key, char* value){};
    virtual bool erase(int64_t key){};
    virtual char* find(int64_t key){};
};
#endif