#include <string>
#include <cstdint>

#include "global.h"
#include "file.h"
#include "table.h"
#include "buffer.h"
#include "index.h"
#include "api.h"

int init_db(int buffer_size){
    if(buffer!=nullptr){
        delete buffer;
    }
    buffer = new Buffer(buffer_size);
    if(table!=nullptr){
        delete table;
    }
    table = new Table();
    return 0;
}

int end_db(){
    delete buffer;
    delete table;
    buffer = nullptr;
    table = nullptr;
}

int open(std::string filename, int index_type){
    int table_id = table->open(filename, index_type);
    if(table->get_file(table_id)->get_header()->get_type()!=index_type){
        table->close(table_id);
        return -1;
    }
    return table_id;
}

int close(int table_id){
    table->close(table_id);
}

static bool bplustree_insert(int table_id, int64_t key, char* value){
    bool result, flag = false;
    pagenum_t root_no;
    File* file = table->get_file(table_id);
    if((root_no = file->get_header()->get_root_page())==0){
        flag = true;
        file->get_header()->set_root_page(root_no = file->alloc_page());
    }

    buffer->pin_page(table_id, root_no);
    bplustree* root = (bplustree*)(buffer->get_page(table_id, root_no));
    if(flag){
        root->set_parent(0);
        buffer->mark_dirty(table_id, root_no);
    }
    result = root->insert(table_id, key, value);
    buffer->unpin_page(table_id, root_no);
    return result;
}

static bool skiplist_insert(int table_id, int64_t key, char* value){
    bool result, flag = false;
    pagenum_t head_no;
    File* file = table->get_file(table_id);
    if((head_no = file->get_header()->get_root_page())==0){
        flag = true;
        file->get_header()->set_root_page(head_no = file->alloc_page());
    }

    buffer->pin_page(table_id, head_no);
    skiplist* head = (skiplist*)(buffer->get_page(table_id, head_no));
    if(flag){
        head->init_node();
        head->set_max_level();
        buffer->mark_dirty(table_id, head_no);
    }
    result = head->insert(table_id, key, value);
    buffer->unpin_page(table_id, head_no);
    return result;
}

bool insert(int table_id, int64_t key, char* value){
    File* file = table->get_file(table_id);
    if(!file){
        return false;
    }
    int type = file->get_header()->get_type();
    if(type == BPLUSTREE){
        return bplustree_insert(table_id, key, value);
    }else if(type == SKIPLIST){
        return skiplist_insert(table_id, key, value);
    }
}

static bool bplustree_erase(int table_id, int64_t key){
    bool result, flag = false;
    File* file = table->get_file(table_id);
    if(!file){
        return false;
    }
    pagenum_t root_no;
    if((root_no = file->get_header()->get_root_page())==0){
        flag = true;
        file->get_header()->set_root_page(root_no = file->alloc_page());
    }

    buffer->pin_page(table_id, root_no);
    bplustree* root = (bplustree*)(buffer->get_page(table_id, root_no));
    if(flag){
        root->set_parent(0);
        buffer->mark_dirty(table_id, root_no);
    }
    result = root->erase(table_id, key);
    buffer->unpin_page(table_id, root_no);
    return result;   
}

static bool skiplist_erase(int table_id, int64_t key){
    bool result, flag = false;
    pagenum_t head_no;
    File* file = table->get_file(table_id);
    if((head_no = file->get_header()->get_root_page())==0){
        flag = true;
        file->get_header()->set_root_page(head_no = file->alloc_page());
    }

    buffer->pin_page(table_id, head_no);
    skiplist* head = (skiplist*)(buffer->get_page(table_id, head_no));
    if(flag){
        head->init_node();
        head->set_max_level();
        buffer->mark_dirty(table_id, head_no);
    }
    result = head->erase(table_id, key);
    buffer->unpin_page(table_id, head_no);
    return result;    
}

bool erase(int table_id, int64_t key){    
    File* file = table->get_file(table_id);
    if(!file){
        return false;
    }
    int type = file->get_header()->get_type();
    if(type == BPLUSTREE){
        return bplustree_erase(table_id, key);
    }else if(type == SKIPLIST){
        return skiplist_erase(table_id, key);
    }
}

static char* bplustree_find(int table_id, int64_t key){
    bool flag = false;
    char* result;
    File* file = table->get_file(table_id);
    pagenum_t root_no;
    if((root_no = file->get_header()->get_root_page())==0){
        flag = true;
        file->get_header()->set_root_page(root_no = file->alloc_page());
    }

    buffer->pin_page(table_id, root_no);
    bplustree* root = (bplustree*)(buffer->get_page(table_id, root_no));
    if(flag){
        root->set_parent(0);
        buffer->mark_dirty(table_id, root_no);
    }
    result = root->find(table_id, key);
    buffer->unpin_page(table_id, root_no);
    return result;
}

static char* skiplist_find(int table_id, int64_t key){
    bool flag = false;
    char* result;
    File* file = table->get_file(table_id);
    pagenum_t head_no;
    if((head_no = file->get_header()->get_root_page())==0){
        flag = true;
        file->get_header()->set_root_page(head_no = file->alloc_page());
    }

    buffer->pin_page(table_id, head_no);
    skiplist* head = (skiplist*)(buffer->get_page(table_id, head_no));
    if(flag){
        head->init_node();
        head->set_max_level();
        buffer->mark_dirty(table_id, head_no);
    }
    result = head->find(table_id, key);
    buffer->unpin_page(table_id, head_no);
    return result;
}

char* find(int table_id, int64_t key){
    File* file = table->get_file(table_id);
    if(!file){
        return nullptr;
    }
    int type = file->get_header()->get_type();
    if(type == BPLUSTREE){
        return bplustree_find(table_id, key);
    }else if(type == SKIPLIST){
        return skiplist_find(table_id, key);
    }
}

static void bplustree_print(int table_id){
    bool flag = false;
    File* file = table->get_file(table_id);
    if(!file){
        return;
    }
    pagenum_t root_no;
    if((root_no = file->get_header()->get_root_page())==0){
        file->get_header()->set_root_page(root_no = file->alloc_page());
    }

    buffer->pin_page(table_id, root_no);
    bplustree* root = (bplustree*)(buffer->get_page(table_id, root_no));
    if(flag){
        root->set_parent(0);
        buffer->mark_dirty(table_id, root_no);
    }
    root->print(table_id);
    buffer->unpin_page(table_id, root_no);
}

static void skiplist_print(int table_id){
    bool flag = false;
    File* file = table->get_file(table_id);
    if(!file){
        return;
    }
    pagenum_t head_no;
    if((head_no = file->get_header()->get_root_page())==0){
        file->get_header()->set_root_page(head_no = file->alloc_page());
        flag = true;
    }

    buffer->pin_page(table_id, head_no);
    skiplist* head = (skiplist*)(buffer->get_page(table_id, head_no));
    if(flag){
        head->init_node();
        head->set_max_level();
        buffer->mark_dirty(table_id, head_no);
    }
    head->print(table_id);
    buffer->unpin_page(table_id, head_no);
}

void print(int table_id){
    File* file = table->get_file(table_id);
    if(!file){
        return;
    }
    int type = file->get_header()->get_type();
    if(type == BPLUSTREE){
        return bplustree_print(table_id);
    }else if(type == SKIPLIST){
        return skiplist_print(table_id);
    }
}

