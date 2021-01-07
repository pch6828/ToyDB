#ifndef __TABLE_H__
#define __TABLE_H__

#include <unordered_map>
#include <string>
#include "file.h"

class Table{
private:
    std::unordered_map<int, File*>files;
    std::unordered_map<std::string, int>idx;
    int cnt;
public:
    ~Table();
    int open(std::string filename, int index);
    void close(int table_id);
    File* get_file(int table_id);
};

extern Table* table;
#endif