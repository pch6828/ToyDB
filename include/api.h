#include <string>
#include <cstdint>
#include "global.h"

#ifndef __API_H__
#define __API_H__

int init_db(int buffer_size);
int end_db();
int open(std::string filename, int index_type);
int close(int table_id);
bool insert(int table_id, int64_t key, char* value);
char* find(int table_id, int64_t key);
bool erase(int table_id, int64_t key);
void print(int table_id);

#endif