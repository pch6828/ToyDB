#include <cstring>
#include <queue>
#include <iostream>
#include <random>

#include "global.h"
#include "file.h"
#include "table.h"
#include "buffer.h"
#include "index.h"

//#define MAX_LEVEL 16
//#define MAX_RECORD 23
//for debug
#define MAX_LEVEL 4
#define MAX_RECORD 4

std::random_device rd;

skiplist::skiplist(){
    this->flag = rd() % MAX_LEVEL;
    this->num_keys = 0;
    memset(this->prev, 0, sizeof(this->prev));
    memset(this->next, 0, sizeof(this->next));
}

//Skip List API #1
//FIND
pagenum_t skiplist::find_node(int table_id, int64_t key, pagenum_t now, int now_lvl){
    for(int i = now_lvl; i >=0; i--){
        if(next[i].get_key()<=key&&next[i].get_page_no()!=0){
            pagenum_t nxt_no = next[i].get_page_no();
            buffer->pin_page(table_id, nxt_no);
            skiplist* nxt = (skiplist*)(buffer->get_page(table_id, nxt_no));
            pagenum_t result = nxt->find_node(table_id, key, nxt_no, i);
            buffer->unpin_page(table_id, nxt_no);
            return result;
        }    
    }
    return now;
}

char* skiplist::find(int table_id, int64_t key){
    char* result = nullptr;
    pagenum_t now = table->get_file(table_id)->get_header()->get_root_page();
    pagenum_t target_no = this->find_node(table_id, key, now, this->flag);

    buffer->pin_page(table_id, target_no);
    skiplist* target = (skiplist*)(buffer->get_page(table_id, target_no));
    for(int i = 0; i < target->num_keys; i++){
        if(target->records[i].get_key()==key){
            result = new char[120];
            memcpy(result, target->records[i].get_value(), 120);
            break;
        }
    }
    buffer->unpin_page(table_id, target_no);
    return result;
}

//Skip List API #2
//INSERT