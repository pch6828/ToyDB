#include <cstring>
#include <queue>
#include <iostream>

#include "global.h"
#include "file.h"
#include "table.h"
#include "buffer.h"
#include "index.h"

//#define MAX_BRANCH 248
//#define MAX_RECORD 31
//for debug
#define MAX_BRANCH 4
#define MAX_RECORD 4

bplustree::bplustree(){
    this->flag = 1;
    this->parent_no = 0;
    this->num_keys = 0;
}

//B+Tree API #1
//FIND
pagenum_t bplustree::find_leaf(int table_id, int64_t key, pagenum_t now){
    if(flag == 1){
        return now;
    }
    for(int i = num_keys-1; i >= 0; i--){
        if(child[i].get_key()<=key){
            buffer->pin_page(table_id, child[i].get_page_no());
            bplustree* nxt = (bplustree*)(buffer->get_page(table_id, child[i].get_page_no()));
            pagenum_t result = nxt->find_leaf(table_id, key, child[i].get_page_no());
            buffer->unpin_page(table_id, child[i].get_page_no());
            return result;
        }
    }
    buffer->pin_page(table_id, leftmost_child);
    bplustree* nxt = (bplustree*)(buffer->get_page(table_id, leftmost_child));
    pagenum_t result = nxt->find_leaf(table_id, key, leftmost_child);
    buffer->unpin_page(table_id, leftmost_child);
    return result;
}

char* bplustree::find(int table_id, int64_t key){
    char* result = nullptr;
    pagenum_t now = table->get_file(table_id)->get_header()->get_root_page();
    pagenum_t target_no = this->find_leaf(table_id, key, now);
    buffer->pin_page(table_id, target_no);
    bplustree* target = (bplustree*)(buffer->get_page(table_id, target_no));

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
    bplustree* right = (bplustree*)(buffer->get_page(table_id, right_no));
    
    right->num_keys = 0;
    right->flag = 1;

    right->right_sibling = left->right_sibling;
    left->right_sibling = right_no;
    bool inserted = false;
    while(idx<MAX_RECORD+1){
        Record& now = left->records[i];
        if(!inserted && (i==left->num_keys||now.get_key()>record.get_key())){
            temp[idx] = record;
            inserted = true;
            
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
        parent = (bplustree*)(buffer->get_page(table_id, parent_no));
        parent->insert_branch(table_id, parent_no, branch);
        buffer->mark_dirty(table_id, parent_no);
        buffer->unpin_page(table_id, parent_no);
    }else{
        parent_no = buffer->alloc_page(table_id);
        pagenum_t left_no = table->get_file(table_id)->get_header()->get_root_page();
        buffer->pin_page(table_id, parent_no);
        parent = (bplustree*)(buffer->get_page(table_id, parent_no));
        parent->init_root(table_id, parent_no, left_no, branch);
        table->get_file(table_id)->get_header()->set_root_page(parent_no);
        buffer->mark_dirty(table_id, parent_no);
        buffer->unpin_page(table_id, parent_no);
    }
    buffer->mark_dirty(table_id, right_no);
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
    page = (bplustree*)(buffer->get_page(table_id, leftmost));
    page->parent_no = now;
    buffer->mark_dirty(table_id, leftmost);
    buffer->unpin_page(table_id, leftmost);

    buffer->pin_page(table_id, branch.get_page_no());
    page = (bplustree*)(buffer->get_page(table_id, branch.get_page_no()));
    page->parent_no = now;
    buffer->mark_dirty(table_id, branch.get_page_no());
    buffer->unpin_page(table_id, branch.get_page_no()); 
}

void bplustree::add_branch(int table_id, pagenum_t now, Branch& branch){
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
    bplustree* page = (bplustree*)(buffer->get_page(table_id, page_no));
    page->parent_no = now;
    buffer->mark_dirty(table_id, page_no);
    buffer->unpin_page(table_id, page_no);
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
    bplustree* right = (bplustree*)(buffer->get_page(table_id, right_no));
    
    right->num_keys = 0;
    right->flag = 0;
    bool inserted = false;
    while(idx<MAX_BRANCH+1){
        Branch& now = left->child[i];
        if(!inserted&&(i==left->num_keys||now.get_key()>branch.get_key())){
            temp[idx] = branch;
            inserted = true;
        }else{
            temp[idx] = now;
            i++;
        }
        idx++;
    }
    pagenum_t page_no;
    for(int i = 0; i < left_cnt; i++){
        left->child[i] = temp[i];
        page_no = temp[i].get_page_no();
        buffer->pin_page(table_id, page_no);
        bplustree* page = (bplustree*)(buffer->get_page(table_id, page_no));
        page->parent_no = left_no;
        buffer->unpin_page(table_id, page_no);
    }
    left->num_keys = left_cnt;
    right->leftmost_child = temp[left_cnt].get_page_no();
    page_no = temp[left_cnt].get_page_no();
    buffer->pin_page(table_id, page_no);
    bplustree* page = (bplustree*)(buffer->get_page(table_id, page_no));
    page->parent_no = right_no;
    buffer->unpin_page(table_id, page_no);
    key = temp[left_cnt].get_key();
    for(int i = left_cnt+1; i < MAX_BRANCH+1; i++){
        right->child[i-left_cnt-1] = temp[i];
        page_no = temp[i].get_page_no();
        buffer->pin_page(table_id, page_no);
        bplustree* page = (bplustree*)(buffer->get_page(table_id, page_no));
        page->parent_no = right_no;
        buffer->unpin_page(table_id, page_no);
    }
    right->num_keys = right_cnt;

    Branch new_branch;
    new_branch.set_key(key);
    new_branch.set_page_no(right_no);

    bplustree* parent;
    pagenum_t parent_no;
    if(left->parent_no){
        parent_no = left->parent_no;
        buffer->pin_page(table_id, parent_no);
        parent = (bplustree*)(buffer->get_page(table_id, parent_no));
        parent->insert_branch(table_id, parent_no, new_branch);
        buffer->mark_dirty(table_id, parent_no);
        buffer->unpin_page(table_id, parent_no);
    }else{
        parent_no = buffer->alloc_page(table_id);
        pagenum_t left_no = table->get_file(table_id)->get_header()->get_root_page();
        buffer->pin_page(table_id, parent_no);
        parent = (bplustree*)(buffer->get_page(table_id, parent_no));
        parent->init_root(table_id, parent_no, left_no, new_branch);
        table->get_file(table_id)->get_header()->set_root_page(parent_no);
        buffer->mark_dirty(table_id, parent_no);
        buffer->unpin_page(table_id, parent_no);
    }
    buffer->mark_dirty(table_id, right_no);
    buffer->unpin_page(table_id, right_no);
}

void bplustree::insert_branch(int table_id, pagenum_t now, Branch& branch){
    if(this->num_keys < MAX_BRANCH){
        this->add_branch(table_id, now, branch);
    }else{
        this->split_internal(table_id, now, branch);
    }
}

bool bplustree::insert(int table_id, int64_t key, char* value){
    Record record(key, value);
    pagenum_t now = table->get_file(table_id)->get_header()->get_root_page();
    pagenum_t target_no = this->find_leaf(table_id, key, now);
    char* temp;
    if((temp = find(table_id, key))!=nullptr){
        delete temp;
        return false;
    }
    buffer->pin_page(table_id, target_no);
    bplustree* target = (bplustree*)(buffer->get_page(table_id, target_no));
    if(target->num_keys < MAX_RECORD){
        target->add_record(record);
    }else{
        target->split_leaf(table_id, record);
    }
    buffer->mark_dirty(table_id, target_no);
    buffer->unpin_page(table_id, target_no);
    return true;
}

//B+Tree API #3
//DELETE
int bplustree::find_idx(pagenum_t page_no){
    if(leftmost_child == page_no){
        return -1;
    }
    for(int i = 0; i < num_keys; i++){
        if(child[i].get_page_no() == page_no){
            return i;
        }
    }
}

void bplustree::remove_record(int64_t key){
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

void bplustree::remove_branch(int table_id, int64_t key){
    bool found = false;
    pagenum_t target_no;
    for(int i = 0; i < this->num_keys; i++){
        if(found){
            child[i-1] = child[i];
        }
        if(child[i].get_key() == key){
            found = true;
            target_no = child[i].get_page_no();
        }
    }
    this->num_keys--;
    buffer->pin_page(table_id, target_no);
    buffer->free_page(table_id, target_no);
    buffer->mark_dirty(table_id, target_no);
    buffer->unpin_page(table_id, target_no);
}

void bplustree::adjust_root(int table_id, pagenum_t now){
    if(num_keys>0){
        return;
    }
    pagenum_t root_no = 0;
    if(!flag){
        root_no = this->leftmost_child;
        buffer->pin_page(table_id, root_no);
        bplustree* root = (bplustree*)(buffer->get_page(table_id, root_no));
        root->parent_no = 0;
        buffer->mark_dirty(table_id, root_no);
        buffer->unpin_page(table_id, root_no);
    }
    table->get_file(table_id)->get_header()->set_root_page(root_no);
    buffer->free_page(table_id, now);
}

void bplustree::redistribute_node(int table_id, pagenum_t left_no, pagenum_t right_no, int idx){
    buffer->pin_page(table_id, left_no);
    buffer->pin_page(table_id, right_no);
    bplustree* parent = this;
    bplustree* left = (bplustree*)(buffer->get_page(table_id, left_no));
    bplustree* right = (bplustree*)(buffer->get_page(table_id, right_no));

    if(left->flag){
        if(idx==-1){
            left->records[0] = right->records[0];
            left->num_keys++;
            right->remove_record(right->records[0].get_key());
            parent->child[0].set_key(right->records[0].get_key());
        }else{
            right->records[0] = left->records[left->num_keys-1];
            right->num_keys++;
            left->remove_record(left->records[left->num_keys-1].get_key());
            parent->child[idx].set_key(right->records[0].get_key());
        }
    }else{
        if(idx==-1){
            Branch branch;
            branch.set_key(parent->child[0].get_key());
            branch.set_page_no(right->leftmost_child);
            left->child[0] = branch;
            left->num_keys++;
            pagenum_t child_no = branch.get_page_no();
            buffer->pin_page(table_id, child_no);
            bplustree* child = (bplustree*)(buffer->get_page(table_id, child_no));
            child->parent_no = left_no;
            buffer->mark_dirty(table_id, child_no);
            buffer->unpin_page(table_id, child_no);
            right->leftmost_child = right->child[0].get_page_no();
            parent->child[0].set_key(right->child[0].get_key());
            for(int i = 1; i < right->num_keys; i++){
                right->child[i-1] = right->child[i];
            }
            right->num_keys--;
        }else{
            Branch branch;
            branch.set_key(parent->child[idx].get_key());
            branch.set_page_no(right->leftmost_child);
            right->child[0] = branch;
            right->num_keys++;
            right->leftmost_child = left->child[left->num_keys-1].get_page_no();
            pagenum_t child_no = right->leftmost_child;
            buffer->pin_page(table_id, child_no);
            bplustree* child = (bplustree*)(buffer->get_page(table_id, child_no));
            child->parent_no = right_no;
            buffer->mark_dirty(table_id, child_no);
            buffer->unpin_page(table_id, child_no);
            parent->child[idx].set_key(left->child[left->num_keys-1].get_key());
            left->num_keys--;
        }
    }
    buffer->mark_dirty(table_id, left_no);
    buffer->mark_dirty(table_id, right_no);
    buffer->unpin_page(table_id, right_no);
    buffer->unpin_page(table_id, left_no);
}

void bplustree::merge_node(int table_id, pagenum_t parent_no, pagenum_t left_no, pagenum_t right_no, int idx){
    int64_t key;
    buffer->pin_page(table_id, left_no);
    buffer->pin_page(table_id, right_no);
    bplustree* parent = this;
    bplustree* left = (bplustree*)(buffer->get_page(table_id, left_no));
    bplustree* right = (bplustree*)(buffer->get_page(table_id, right_no));

    if(left->flag){
        if(idx==-1){
            left->records[0] = right->records[0];
            left->right_sibling = right->right_sibling;
            left->num_keys++;
            key = parent->child[0].get_key();
        }else{
            left->right_sibling = right->right_sibling;
            key = parent->child[idx].get_key();
        }
    }else{
        if(idx==-1){
            Branch branch;
            branch.set_key(parent->child[0].get_key());
            branch.set_page_no(right->leftmost_child);
            left->child[0] = branch;
            left->child[1] = right->child[0];
            pagenum_t child_no;
            bplustree* child;
            child_no = right->leftmost_child;
            buffer->pin_page(table_id, child_no);
            child = (bplustree*)(buffer->get_page(table_id, child_no));
            child->parent_no = left_no;
            buffer->mark_dirty(table_id, child_no);
            buffer->unpin_page(table_id, child_no);
            child_no = right->child[0].get_page_no();
            buffer->pin_page(table_id, child_no);
            child = (bplustree*)(buffer->get_page(table_id, child_no));
            child->parent_no = left_no;
            buffer->mark_dirty(table_id, child_no);
            buffer->unpin_page(table_id, child_no);
            left->num_keys+=2;
            key = parent->child[0].get_key();
        }else{
            Branch branch;
            branch.set_key(parent->child[0].get_key());
            branch.set_page_no(right->leftmost_child);
            left->child[1] = branch;
            pagenum_t child_no;
            bplustree* child;
            child_no = right->leftmost_child;
            buffer->pin_page(table_id, child_no);
            child = (bplustree*)(buffer->get_page(table_id, child_no));
            child->parent_no = left_no;
            buffer->mark_dirty(table_id, child_no);
            buffer->unpin_page(table_id, child_no);
            left->num_keys++;
            key = parent->child[0].get_key();
        }
    }

    erase_entry(table_id, parent_no, key);
    buffer->mark_dirty(table_id, left_no);
    buffer->mark_dirty(table_id, right_no);
    buffer->unpin_page(table_id, right_no);
    buffer->unpin_page(table_id, left_no);
}

void bplustree::erase_entry(int table_id, pagenum_t now, int64_t key){
    if(flag){
        this->remove_record(key);
    }else{
        this->remove_branch(table_id, key);
    }
    if(this->parent_no==0){
        this->adjust_root(table_id, now);
        return;
    }
    if(num_keys > 0){
        return;
    }
    pagenum_t parent_no = this->parent_no, neighbor_no, left_no, right_no;
    buffer->pin_page(table_id, parent_no);
    bplustree* parent = (bplustree*)(buffer->get_page(table_id, parent_no));
    int idx = parent->find_idx(now);
    if(idx==-1){    
        neighbor_no = parent->child[0].get_page_no();
        left_no = now;
        right_no = neighbor_no;
    }else if(idx==0){
        neighbor_no = parent->leftmost_child;
        left_no = neighbor_no;
        right_no = now;
    }else{
        neighbor_no = parent->child[idx-1].get_page_no();
        left_no = neighbor_no;
        right_no = now;
    }
    buffer->pin_page(table_id, neighbor_no);
    bplustree* neighbor = (bplustree*)(buffer->get_page(table_id, neighbor_no));
    
    if(neighbor->num_keys>1){
        parent->redistribute_node(table_id, left_no, right_no, idx);
    }else{
        parent->merge_node(table_id, parent_no, left_no, right_no, idx);
    }
    buffer->mark_dirty(table_id, neighbor_no);
    buffer->mark_dirty(table_id, parent_no);
    buffer->unpin_page(table_id, neighbor_no);
    buffer->unpin_page(table_id, parent_no);
}

bool bplustree::erase(int table_id, int64_t key){
    pagenum_t now = table->get_file(table_id)->get_header()->get_root_page();
    pagenum_t target_no = this->find_leaf(table_id, key, now);
    char* temp;
    if((temp = find(table_id, key))==nullptr){
        return false;
    }
    delete temp;
    buffer->pin_page(table_id, target_no);
    bplustree* target = (bplustree*)(buffer->get_page(table_id, target_no));
    target->erase_entry(table_id, target_no, key);
    buffer->mark_dirty(table_id, target_no);
    buffer->unpin_page(table_id, target_no);
    return true;
}

//B+Tree API #4
//PRINT
int bplustree::height(int table_id){
    int result = 0;
    if(this->parent_no==0){
        return result;
    }
    buffer->pin_page(table_id, this->parent_no);
    bplustree* target = (bplustree*)(buffer->get_page(table_id, this->parent_no));
    result = target->height(table_id)+1;
    buffer->unpin_page(table_id, this->parent_no);
    return result;
}

void bplustree::print(int table_id){
    std::queue<pagenum_t>pages;
    int h = 0;
    bplustree* now = this;
    if(!now->flag){
        pages.push(now->leftmost_child);
    }
    for(int i = 0; i < now->num_keys; i++){
        if(now->flag){
            std::cout<<now->records[i].get_key()<<" ";
        }else{
            std::cout<<now->child[i].get_key()<<" ";
            pages.push(now->child[i].get_page_no());
        }
    }
    std::cout<<"|";
    while(!pages.empty()){
        pagenum_t now_no = pages.front();
        pages.pop();
        buffer->pin_page(table_id, now_no);
        now = (bplustree*)(buffer->get_page(table_id, now_no));
        int now_h = now->height(table_id);
        if(now_h>h){
            h = now_h;
            std::cout<<std::endl;
        }
        if(!now->flag){
            pages.push(now->leftmost_child);
        }
        for(int i = 0; i < now->num_keys; i++){
            if(now->flag){
                std::cout<<now->records[i].get_key()<<" ";
            }else{
                std::cout<<now->child[i].get_key()<<" ";
                pages.push(now->child[i].get_page_no());
            }
        }
        std::cout<<"|";
        buffer->unpin_page(table_id, now_no);
    }
}