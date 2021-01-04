#include <string>
#include "file.h"
#include "table.h"

int Table::open(std::string filename, int index = BPLUSTREE){
    if(this->idx[filename]){
        return this->idx[filename];
    }
    while(this->files.count(this->cnt++));
    File* file = new File(filename, index);
    this->idx[filename] = this->cnt;
    this->files[this->cnt] = file;
    return this->cnt;
}

void Table::close(int table_id){
    File* file = files[table_id];
    idx.erase(file->get_filename());
    files.erase(table_id);
    delete file;
}

File* Table::get_file(int table_id){
    return this->files[table_id];
}