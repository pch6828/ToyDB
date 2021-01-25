#include <string>
#include <iostream>
#include <vector>
#include <cstdint>
#include "api.h"
#include "index.h"
#include "iterator.h"

//using namespace std;

int main(){
    std::string cmd;
    init_db(1000);
    std::vector<iterator*>iter;
    iter.push_back(nullptr);
    while(true){
        std::cin>>cmd;
        if(cmd=="open"){
            std::string filename;
            std::cin>>filename;
            int table_id = open(filename, BPLUSTREE);
            iter.push_back(new bplustree_iterator(table_id));
            std::cout<<"open result : "<<table_id<<std::endl;
        }else if(cmd=="close"){
            int table_id;
            std::cin>>table_id;
            std::cout<<"close result : "<<close(table_id)<<std::endl;
        }else if(cmd=="quit"){
            end_db();
            std::cout<<"bye!"<<std::endl;
            return 0;
        }else if(cmd=="begin"){
            int table_id;
            std::cin>>table_id;
            iter[table_id]->set_iter_begin();
        }else if(cmd=="from"){
            int table_id;
            std::cin>>table_id;
            int64_t key;
            std::cin>>key;
            iter[table_id]->set_iter_from(key);
        }else if(cmd=="next"){
            int table_id;
            std::cin>>table_id;
            Record* result = iter[table_id]->next();
            if(result){
                std::cout<<result->get_key()<<std::endl;
            }else{
                std::cout<<"null"<<std::endl;
            }            
        }
    }
}