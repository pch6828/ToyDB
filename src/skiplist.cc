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
//#define MAX_RECORD 27
//for debug
#define MAX_LEVEL 4
#define MAX_RECORD 4

std::random_device rd;

skiplist::skiplist(){
    this->flag = rd() % MAX_LEVEL + 1;
    this->num_keys = 0;
    memset(this->prev, 0, sizeof(this->prev));
    memset(this->next, 0, sizeof(this->next));
}

void skiplist::init_node(){
    this->flag = rd() % MAX_LEVEL + 1;
    this->num_keys = 0;
    memset(this->prev, 0, sizeof(this->prev));
    memset(this->next, 0, sizeof(this->next));
}

void skiplist::set_max_level(){
    this->flag = MAX_LEVEL;
}
//Skip List API #1
//FIND
pagenum_t skiplist::find_node(int table_id, int64_t key, pagenum_t iter, int now_lvl){
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
    return iter;
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
void skiplist::add_record(Record& record){
    int idx = num_keys;
    for(int i = 0; i < num_keys; i++){
        if(records[i].get_key()>record.get_key()){
            idx = i;
            break;
        }
    }    
    for(int i = num_keys-1; i >=idx; i--){
        records[i+1] = records[i];
    }
    records[idx] = record;
    num_keys++;
}

pagenum_t skiplist::split_node(int table_id, Record& record){
    pagenum_t newnode_no = buffer->alloc_page(table_id);
    Record temp[MAX_RECORD+1];
    int idx = 0, i = 0;
    int now_cnt = (MAX_RECORD+1)/2;
    int new_cnt = (MAX_RECORD+1)-now_cnt;
    bool inserted = false;

    while(idx<MAX_RECORD+1){
        Record& now = this->records[i];
        if(!inserted && (i==this->num_keys||now.get_key()>record.get_key())){
            temp[idx] = record;
            inserted = true;
            
        }else{
            temp[idx] = now;
            i++;
        }
        idx++;
    }
    for(int i = 0; i < now_cnt; i++){
        this->records[i] = temp[i];
    }
    this->num_keys = now_cnt;
    buffer->pin_page(table_id, newnode_no);
    skiplist* newnode = (skiplist*)(buffer->get_page(table_id, newnode_no));
    newnode->init_node();
    for(int i = now_cnt; i < MAX_RECORD+1; i++){
        newnode->records[i-now_cnt] = temp[i];
    }
    newnode->num_keys = new_cnt;
    buffer->mark_dirty(table_id, newnode_no);
    buffer->unpin_page(table_id, newnode_no);
    return newnode_no;
}

void skiplist::insert_in_list(int table_id, pagenum_t iter, Branch& branch, int now_lvl){
    buffer->pin_page(table_id, iter);
    skiplist* now = (skiplist*)(buffer->get_page(table_id, iter));
    if(now->next[now_lvl-1].get_page_no()&&now->next[now_lvl-1].get_key() < this->records[0].get_key()){
        pagenum_t nxt_iter = now->next[now_lvl-1].get_page_no();
        buffer->unpin_page(table_id, iter);
        insert_in_list(table_id, nxt_iter, branch, now_lvl);
        return;
    }else{
        this->next[now_lvl-1] = now->next[now_lvl-1];
        if(now->next[now_lvl-1].get_page_no()){
            pagenum_t nxt_iter = now->next[now_lvl-1].get_page_no();
            buffer->pin_page(table_id, nxt_iter);
            skiplist* nxt = (skiplist*)(buffer->get_page(table_id, nxt_iter));
            nxt->prev[now_lvl-1] = branch;
            buffer->mark_dirty(table_id, nxt_iter);
            buffer->unpin_page(table_id, nxt_iter);
        }
        this->prev[now_lvl-1].set_page_no(iter);
        this->prev[now_lvl-1].set_key(now->records[0].get_key());
        now->next[now_lvl-1] = branch;
        now_lvl--;
        buffer->mark_dirty(table_id, iter);
        buffer->unpin_page(table_id, iter);
        if(now_lvl){
            insert_in_list(table_id, iter, branch, now_lvl);
        }
    }
}

void skiplist::adjust_head(int table_id, pagenum_t now, Record& record){
    if(num_keys<MAX_RECORD){
        this->add_record(record);
        if(this->records[0].get_key()==record.get_key()){
            int64_t key = record.get_key();
            for(int i = 0; i < flag; i++){
                pagenum_t nxt_no = next[i].get_page_no();
                buffer->pin_page(table_id, nxt_no);
                skiplist* nxt = (skiplist*)(buffer->get_page(table_id, nxt_no));
                nxt->prev[i].set_key(key);
                buffer->mark_dirty(table_id, nxt_no);
                buffer->unpin_page(table_id, nxt_no);
            }
        }
        return;
    }

    pagenum_t newnode_no = split_node(table_id, record);
    buffer->pin_page(table_id, newnode_no);
    skiplist* newnode = (skiplist*)(buffer->get_page(table_id, newnode_no));
    Branch branch;
    branch.set_key(newnode->records[0].get_key());
    branch.set_page_no(newnode_no);
    newnode->insert_in_list(table_id, now, branch, newnode->flag);
    buffer->mark_dirty(table_id, newnode_no);
    buffer->unpin_page(table_id, newnode_no);
    
    if(this->records[0].get_key()==record.get_key()){
        int64_t key = record.get_key();
        for(int i = 0; i < flag; i++){
            pagenum_t nxt_no = next[i].get_page_no();
            buffer->pin_page(table_id, nxt_no);
            skiplist* nxt = (skiplist*)(buffer->get_page(table_id, nxt_no));
            nxt->prev[i].set_key(key);
            buffer->mark_dirty(table_id, nxt_no);
            buffer->unpin_page(table_id, nxt_no);
        }
    }
}

bool skiplist::insert(int table_id, int64_t key, char* value){
    Record record(key, value);
    pagenum_t now = table->get_file(table_id)->get_header()->get_root_page();
    pagenum_t target_no = this->find_node(table_id, key, now, flag);
    pagenum_t newnode_no = 0;
    char* temp;
    if((temp = this->find(table_id, key))!=nullptr){
        delete temp;
        return false;
    }
    if(now==target_no){
        this->adjust_head(table_id, now, record);
        buffer->mark_dirty(table_id, now);
        return true;
    }
    buffer->pin_page(table_id, target_no);
    skiplist* target = (skiplist*)(buffer->get_page(table_id, target_no));
    if(target->num_keys<MAX_RECORD){
        target->add_record(record);
    }else{
        newnode_no = target->split_node(table_id, record);
    }
    buffer->mark_dirty(table_id, target_no);
    buffer->unpin_page(table_id, target_no);

    if(newnode_no){
        buffer->pin_page(table_id, newnode_no);
        skiplist* newnode = (skiplist*)(buffer->get_page(table_id, newnode_no));
        Branch branch;
        branch.set_key(newnode->records[0].get_key());
        branch.set_page_no(newnode_no);
        newnode->insert_in_list(table_id, now, branch, newnode->flag);
        buffer->mark_dirty(table_id, newnode_no);
        buffer->unpin_page(table_id, newnode_no);
    }
    return true;
}

//Skip List API #3
//DELETE
void skiplist::remove_record(int64_t key){
    bool found = false;
    for(int i = 0; i < this->num_keys; i++){
        if(found){
            records[i-1] = records[i];
        }
        if(records[i].get_key() == key){
            found = true;
        }
    }  
    this->num_keys--;
}

void skiplist::reconnect_list(int table_id){
    for(int i = 0; i < flag; i++){
        pagenum_t pred_no = prev[i].get_page_no();
        pagenum_t succ_no = next[i].get_page_no();
        if(pred_no){
            buffer->pin_page(table_id, pred_no);
            skiplist* pred = (skiplist*)(buffer->get_page(table_id, pred_no));
            pred->next[i] = this->next[i];
            buffer->mark_dirty(table_id, pred_no);
            buffer->unpin_page(table_id, pred_no);
        }
        if(succ_no){
            buffer->pin_page(table_id, succ_no);
            skiplist* succ = (skiplist*)(buffer->get_page(table_id, succ_no));
            succ->prev[i] = this->prev[i];
            buffer->mark_dirty(table_id, succ_no);
            buffer->unpin_page(table_id, succ_no);
        }
    }
}

void skiplist::reset_head(int table_id){
    pagenum_t newhead_no = next[0].get_page_no();
    if(newhead_no){
        buffer->pin_page(table_id, newhead_no);
        skiplist* newhead = (skiplist*)(buffer->get_page(table_id, newhead_no));
        int64_t key = next[0].get_key();
        Branch branch;
        branch.set_key(key);
        branch.set_page_no(newhead_no);
        for(int i = newhead->flag; i < MAX_LEVEL; i++){
            newhead->next[i] = next[i];
            pagenum_t temp_no = next[i].get_page_no();
            if(temp_no){
                buffer->pin_page(table_id, temp_no);
                skiplist* temp = (skiplist*)(buffer->get_page(table_id, temp_no));
                temp->prev[i] = branch;
                buffer->mark_dirty(table_id, temp_no);
                buffer->unpin_page(table_id, temp_no);
            }
        }
        newhead->set_max_level();
        buffer->mark_dirty(table_id, newhead_no);
        buffer->unpin_page(table_id, newhead_no);
    }
    table->get_file(table_id)->get_header()->set_root_page(newhead_no);
}

bool skiplist::erase(int table_id, int64_t key){
    pagenum_t now = table->get_file(table_id)->get_header()->get_root_page();
    pagenum_t target_no = this->find_node(table_id, key, now, flag);
    pagenum_t newnode_no = 0;
    char* temp;
    if((temp = this->find(table_id, key))==nullptr){
        return false;
    }
    delete temp;
    buffer->pin_page(table_id, target_no);
    skiplist* target = (skiplist*)(buffer->get_page(table_id, target_no)); 
    target->remove_record(key);
    if(target->num_keys==0){
        if(target_no==now){
            target->reset_head(table_id);
        }else{
            target->reconnect_list(table_id);
        }
        buffer->free_page(table_id, target_no);
    }
    buffer->mark_dirty(table_id, target_no);
    buffer->unpin_page(table_id, target_no);
    return true;
}

//Skip List API #4
//PRINT
void skiplist::dump_node(){
    std::cout<<"level: "<<this->flag<<std::endl;
    std::cout<<"prev : ";
    for(int i = 0; i < this->flag; i++){
        std::cout<<this->prev[i].get_key()<<" ";
    }
    std::cout<<std::endl;
    std::cout<<"next : ";
    for(int i = 0; i < this->flag; i++){
        std::cout<<this->next[i].get_key()<<" ";
    }
    std::cout<<std::endl;
    std::cout<<"records : ";
    for(int i = 0; i < this->num_keys; i++){
        std::cout<<this->records[i].get_key()<<" ";
    }
    std::cout<<std::endl;
}

void skiplist::print(int table_id){
    pagenum_t nxt_no = 0;
    skiplist* now = this;
    while(true){
        now->dump_node();
        if(nxt_no){
            buffer->unpin_page(table_id, nxt_no);
        }
        nxt_no = now->next[0].get_page_no();
        if(nxt_no==0){
            return;
        }
        buffer->pin_page(table_id, nxt_no);
        now = (skiplist*)(buffer->get_page(table_id, nxt_no));
    }
}