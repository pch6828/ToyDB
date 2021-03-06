#ifndef __FILE_H__
#define __FILE_H__

#include <string>
#include "global.h"

class File{
private:
    int fd;
    std::string filename;
    HeaderPage* header;
public:
    File(std::string filename, int index);
    ~File();
    std::string get_filename();

    void write_page(pagenum_t page_no, basic_page* page);
    void read_page(pagenum_t page_no, basic_page* page);
    pagenum_t alloc_page();
    void free_page(pagenum_t page_no, basic_page* page);

    void write_header();
    void read_header();
    HeaderPage* get_header();
};

class Page : public basic_page{
private:
    char reserved[3976];
public:
    virtual bool insert(int table_id, int64_t key, char* value){return true;}
    virtual bool erase(int table_id, int64_t key){return true;}
    virtual char* find(int table_id, int64_t key){return nullptr;}
    virtual void print(int table_id){return;}
};
#endif