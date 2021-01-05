#include <cstring>

#include "global.h"
#include "file.h"
#include "table.h"
#include "buffer.h"
#include "index.h"

#define MAX_BRANCH 248
#define MAX_RECORD 31

//B+Tree API #1
//FIND
pagenum_t bplustree::find_leaf(int table_id, int64_t key, pagenum_t now){
    if(flag == 1){
        return now;
    }
    for(int i = num_keys-1; i >= 0; i--){
        if(child[i].get_key()<=key){
            buffer->pin_page(table_id, child[i].get_page_no());
            bplustree* nxt = dynamic_cast<bplustree*>(buffer->get_page(table_id, child[i].get_page_no()));
            pagenum_t result = nxt->find_leaf(table_id, key, child[i].get_page_no());
            buffer->unpin_page(table_id, child[i].get_page_no());
            return result;
        }
    }
    buffer->pin_page(table_id, leftmost_child);
    bplustree* nxt = dynamic_cast<bplustree*>(buffer->get_page(table_id, leftmost_child));
    pagenum_t result = nxt->find_leaf(table_id, key, leftmost_child);
    buffer->unpin_page(table_id, leftmost_child);
    return result;
}

char* bplustree::find(int table_id, int64_t key){
    char* result = nullptr;
    pagenum_t now = table->get_file(table_id)->get_header()->get_root_page();
    pagenum_t target_no = this->find_leaf(table_id, key, now);
    buffer->pin_page(table_id, target_no);
    bplustree* target = dynamic_cast<bplustree*>(buffer->get_page(table_id, target_no));

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

//B+Tree API #2
//INSERT
void bplustree::add_record(Record& record){
    int idx = num_keys;
    for(int i = 0; i < num_keys; i++){
        if(records[i].get_key()>record.get_key()){
            idx = i;
            break;
        }
    }
    for(int i = num_keys-1; i >= idx; i--){
        records[i+1] = records[i];
    }
    records[idx] = record;
    num_keys++;
}

void bplustree::split_leaf(int table_id, Record& record){
    Record temp[MAX_RECORD+1];
    int idx = 0, i = 0;
    int left_cnt = (MAX_RECORD+1)/2;
    int right_cnt = MAX_RECORD+1-left_cnt;
    pagenum_t right_no = buffer->alloc_page(table_id);
    bplustree* left = this;
    buffer->pin_page(table_id, right_no);
    bplustree* right = dynamic_cast<bplustree*>(buffer->get_page(table_id, right_no));
    
    right->num_keys = 0;
    right->flag = 1;

    right->right_sibling = left->right_sibling;
    left->right_sibling = right_no;

    while(idx<MAX_RECORD+1){
        Record& now = left->records[i];
        if(now.get_key()>record.get_key()){
            temp[idx] = record;
        }else{
            temp[idx] = now;
            i++;
        }
        idx++;
    }
    for(int i = 0; i < left_cnt; i++){
        left->records[i] = temp[i];
    }
    left->num_keys = left_cnt;
    for(int i = left_cnt; i < MAX_RECORD+1; i++){
        right->records[i-left_cnt] = temp[i];
    }
    right->num_keys = right_cnt;

    Branch branch;
    branch.set_key(right->records[0].get_key());
    branch.set_page_no(right_no);

    bplustree* parent;
    pagenum_t parent_no;
    if(left->parent_no){
        parent_no = left->parent_no;
        buffer->pin_page(table_id, parent_no);
        parent = dynamic_cast<bplustree*>(buffer->get_page(table_id, parent_no));
        parent->insert_branch(table_id, parent_no, branch);
        buffer->unpin_page(table_id, parent_no);
    }else{
        parent_no = buffer->alloc_page(table_id);
        pagenum_t left_no = table->get_file(table_id)->get_header()->get_root_page();
        buffer->pin_page(table_id, parent_no);
        parent = dynamic_cast<bplustree*>(buffer->get_page(table_id, parent_no));
        parent->init_root(table_id, parent_no, left_no, branch);
        table->get_file(table_id)->get_header()->set_root_page(parent_no);
        buffer->unpin_page(table_id, parent_no);
    }
    
    buffer->unpin_page(table_id, right_no);
}

void bplustree::init_root(int table_id, pagenum_t now, pagenum_t leftmost, Branch& branch){
    this->parent_no = 0;
    this->num_keys = 1;
    this->flag = 0;
    this->leftmost_child = leftmost;
    this->child[0] = branch;

    bplustree* page;

    buffer->pin_page(table_id, leftmost);
    page = dynamic_cast<bplustree*>(buffer->get_page(table_id, leftmost));
    page->parent_no = now;
    buffer->unpin_page(table_id, leftmost);

    buffer->pin_page(table_id, branch.get_page_no());
    page = dynamic_cast<bplustree*>(buffer->get_page(table_id, branch.get_page_no()));
    page->parent_no = now;
    buffer->unpin_page(table_id, branch.get_page_no()); 
}

void bplustree::add_branch(pagenum_t now, Branch& branch){
    int idx = num_keys;
    for(int i = 0; i < num_keys; i++){
        if(child[i].get_key()>branch.get_key()){
            idx = i;
            break;
        }
    }
    for(int i = num_keys-1; i >= idx; i--){
        child[i+1] = child[i];
    }
    child[idx] = branch;
    num_keys++;
    pagenum_t page_no = branch.get_page_no();
    buffer->pin_page(table_id, page_no);
    bplustree* page = dynamic_cast<bplustree*>(buffer->get_page(table_id, page_no));
    page->parent_no = now;
    buffer->unpin_page(table_id, child_no);
}

int bplustree::find_idx(pagenum_t page_no){
    if(leftmost_child == page_no){
        return -1;
    }
    for(int i = 0; i < num_keys; i++){
        if(child[i].get_key() == page_no){
            return i;
        }
    }
}

void bplustree::split_internal(int table_id, pagenum_t left_no, Branch& branch){
    Branch temp[MAX_BRANCH+1];
    int idx = 0, i = 0;
    int left_cnt = (MAX_BRANCH+1)/2;
    int right_cnt = MAX_BRANCH-left_cnt;
    int64_t key;
    pagenum_t right_no = buffer->alloc_page(table_id);
    bplustree* left = this;
    buffer->pin_page(table_id, right_no);
    bplustree* right = dynamic_cast<bplustree*>(buffer->get_page(table_id, right_no));
    
    right->num_keys = 0;
    right->flag = 0;

    while(idx<MAX_BRANCH+1){
        Branch& now = left->child[i];
        if(now.get_key()>branch.get_key()){
            temp[idx] = branch;
        }else{
            temp[idx] = now;
            i++;
        }
        idx++;
    }
    for(int i = 0; i < left_cnt; i++){
        left->child[i] = temp[i];
    }
    left->num_keys = left_cnt;
    right->leftmost_child = temp[left_cnt].get_page_no();
    key = temp[left_cnt].get_key();
    for(int i = left_cnt+1; i < MAX_BRANCH+1; i++){
        right->child[i-left_cnt-1] = temp[i];
    }
    right->num_keys = right_cnt;

    Branch branch;
    branch.set_key(key);
    branch.set_page_no(right_no);

    bplustree* parent;
    pagenum_t parent_no;
    if(left->parent_no){
        parent_no = left->parent_no;
        buffer->pin_page(table_id, parent_no);
        parent = dynamic_cast<bplustree*>(buffer->get_page(table_id, parent_no));
        parent->insert_branch(table_id, parent_no, branch);
        buffer->unpin_page(table_id, parent_no);
    }else{
        parent_no = buffer->alloc_page(table_id);
        pagenum_t left_no = table->get_file(table_id)->get_header()->get_root_page();
        buffer->pin_page(table_id, parent_no);
        parent = dynamic_cast<bplustree*>(buffer->get_page(table_id, parent_no));
        parent->init_root(table_id, parent_no, left_no, branch);
        table->get_file(table_id)->get_header()->set_root_page(parent_no);
        buffer->unpin_page(table_id, parent_no);
    }
    
    buffer->unpin_page(table_id, right_no);
}

void bplustree::insert_branch(int table_id, pagenum_t now, Branch& branch){
    if(this->num_keys < MAX_BRANCH){
        this->add_branch(now, branch);
    }else{
        this->split_internal(table_id, now, branch);
    }
}

bool bplustree::insert(int table_id, int64_t key, char* value){
    Record record(key, value);
    pagenum_t now = table->get_file(table_id)->get_header()->get_root_page();
    pagenum_t target = this->find_leaf(table_id, key, now);
    if(find(table_id, key)!=nullptr){
        return false;
    }
    buffer->pin_page(table_id, target_no);
    bplustree* target = dynamic_cast<bplustree*>(buffer->get_page(table_id, target_no));
    if(target->num_keys < MAX_RECORD){
        target->add_record(record);
    }else{
        target->split_leaf(table_id, record);
    }
    buffer->unpin_page(table_id, target_no);
    return true;
}