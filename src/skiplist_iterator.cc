#include "global.h"
#include "file.h"
#include "table.h"
#include "buffer.h"
#include "index.h"
#include "iterator.h"

skiplist_iterator::skiplist_iterator(int table_id){
    this->table_id = table_id;
    idx = 0;
    page_no = table->get_file(table_id)->get_header()->get_root_page();
}

void skiplist_iterator::set_iter_begin(){
    idx = 0;
    page_no = table->get_file(table_id)->get_header()->get_root_page();
}

void skiplist_iterator::set_iter_from(int64_t key){
    pagenum_t head_no = table->get_file(table_id)->get_header()->get_root_page();
    buffer->pin_page(table_id, head_no);
    skiplist* head = (skiplist*)(buffer->get_page(table_id, head_no));
    page_no = head->find_node(table_id, key, head_no, head->flag);
    buffer->unpin_page(table_id, head_no);
    
    buffer->pin_page(table_id, page_no);
    skiplist* page = (skiplist*)(buffer->get_page(table_id, page_no));
    idx = page->num_keys;
    for(int i = 0; i < page->num_keys; i++){
        if(page->records[i].get_key()>=key){
            idx = i;
            break;
        }
    }
    if(idx==page->num_keys){
        idx = 0;
        page_no = page->next[0].get_page_no();
    }
    buffer->unpin_page(table_id, page_no);
}

Record* skiplist_iterator::next(){
    Record* result = nullptr;
    pagenum_t next_no = page_no;
    if(page_no==0){
        return result;
    }
    buffer->pin_page(table_id, page_no);
    skiplist* page = (skiplist*)(buffer->get_page(table_id, page_no));
    result = new Record(page->records[idx++]);
    if(idx==page->num_keys){
        idx = 0;
        next_no = page->next[0].get_page_no();
    }
    buffer->unpin_page(table_id, page_no);
    page_no = next_no;
    return result;
}