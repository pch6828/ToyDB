#include "global.h"
#include "file.h"
#include "table.h"
#include "buffer.h"
#include "index.h"
#include "iterator.h"

bplustree_iterator::bplustree_iterator(int table_id){
    bplustree* page;

    this->table_id = table_id;
    idx = 0;
    page_no = table->get_file(table_id)->get_header()->get_root_page();
    while(true){
        pagenum_t temp_no = 0;
        buffer->pin_page(table_id, page_no);
        page = (bplustree*)(buffer->get_page(table_id, page_no));
        if(page->flag==0){
            temp_no = page->leftmost_child;
        }
        buffer->unpin_page(table_id, page_no);
        if(!temp_no){
            break;
        }
        page_no = temp_no;
    }
}

void bplustree_iterator::set_iter_begin(){
    bplustree* page;
    idx = 0;
    page_no = table->get_file(table_id)->get_header()->get_root_page();
    while(true){
        pagenum_t temp_no = 0;
        buffer->pin_page(table_id, page_no);
        page = (bplustree*)(buffer->get_page(table_id, page_no));
        if(page->flag==0){
            temp_no = page->leftmost_child;
        }
        buffer->unpin_page(table_id, page_no);
        if(!temp_no){
            break;
        }
        page_no = temp_no;
    }
}

void bplustree_iterator::set_iter_from(int64_t key){
    pagenum_t root_no = table->get_file(table_id)->get_header()->get_root_page();
    buffer->pin_page(table_id, root_no);
    bplustree* root = (bplustree*)(buffer->get_page(table_id, root_no));
    pagenum_t target_no = root->find_leaf(table_id, key, root_no);
    page_no = target_no;
    buffer->unpin_page(table_id, root_no);
    buffer->pin_page(table_id, target_no);
    bplustree* target = (bplustree*)(buffer->get_page(table_id, target_no));
    idx = target->num_keys;
    for(int i = 0; i < target->num_keys; i++){
        if(target->records[i].get_key()>=key){
            idx = i;
            break;
        }
    }
    if(idx==target->num_keys){
        idx = 0;
        page_no = target->right_sibling;
    }
    buffer->unpin_page(table_id, target_no);
}

Record* bplustree_iterator::next(){
    Record* result = nullptr;
    pagenum_t next_no = page_no;
    if(page_no==0){
        return result;
    }
    buffer->pin_page(table_id, page_no);
    bplustree* page = (bplustree*)(buffer->get_page(table_id, page_no));
    result = new Record(page->records[idx++]);
    if(idx==page->num_keys){
        idx = 0;
        next_no = page->right_sibling;
    }
    buffer->unpin_page(table_id, page_no);
    page_no = next_no;
    return result;
}

