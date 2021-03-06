#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <string>
#include <iostream>
#include "global.h"
#include "file.h"

File::File(std::string filename, int index){
    this->filename = filename;
    this->fd = open(this->filename.c_str(), O_RDWR|O_SYNC|O_CREAT, 0777);
    if(this->fd < 0){
        std::cerr << "File Open Error!" << std::endl;
    }
    this->header = new HeaderPage();
    this->read_header();

    if(this->header->get_size()==1){
        delete this->header;
        this->header = new HeaderPage(index);
    }
}

File::~File(){
    write_header();
    delete header;
    close(fd);
}

std::string File::get_filename(){
    return this->filename;
}

void File::write_page(pagenum_t page_no, basic_page* page){
    pwrite(this->fd, page, PAGE_SIZE, PAGE_SIZE*page_no);
}

void File::read_page(pagenum_t page_no, basic_page* page){
    pread(this->fd, page, PAGE_SIZE, PAGE_SIZE*page_no);
}

pagenum_t File::alloc_page(){
    pagenum_t page_no = this->header->get_free_page();
    if(page_no){
        Page* page = new Page;
        this->read_page(page_no, page);
        this->header->set_free_page(page->get_parent());
        delete page;
        return page_no;
    }

    page_no = this->header->get_size();
    this->header->increase_page();
    return page_no; 
}

void File::free_page(pagenum_t page_no, basic_page* page){
    page->set_parent(this->header->get_free_page());
    this->write_page(page_no, page);
    this->header->set_free_page(page_no);
}

void File::write_header(){
    pwrite(this->fd, this->header, PAGE_SIZE, 0);
}

void File::read_header(){
    pread(this->fd, this->header, PAGE_SIZE, 0);
}

HeaderPage* File::get_header(){
    return this->header;
}