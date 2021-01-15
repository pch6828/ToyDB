#include <string>
#include <iostream>
#include <vector>
#include <cstdint>
#include "api.h"
#include "index.h"

//using namespace std;

void dump_table(std::vector<int>&tables){
    for(int id : tables){
        std::cout<<id<<" ";
    }
    std::cout<<std::endl;
}

int main(){
    int buffer_size;
    std::string cmd;
    std::vector<int>tables;
    std::cout<<"input buffer_size (positive integer) > ";
    std::cin>>buffer_size;
    init_db(buffer_size);

    while(true){
        std::cin>>cmd;
        if(cmd=="insert"){
            int table_id;
            int64_t key;
            char value[120];
            std::cin>>table_id>>key>>value;
            std::cout<<"insert result : "<<insert(table_id, key, value)<<std::endl;
        }else if(cmd=="find"){
            int table_id;
            int64_t key;
            std::cin>>table_id>>key;
            std::cout<<"search result : "<<find(table_id, key)<<std::endl;
        }else if(cmd=="delete"){
            int table_id;
            int64_t key;
            std::cin>>table_id>>key;
            std::cout<<"delete result : "<<erase(table_id, key)<<std::endl;
        }else if(cmd=="print"){
            int table_id;
            std::cin>>table_id;
            print(table_id);
            std::cout<<std::endl;
        }else if(cmd=="open"){
            std::string filename;
            std::cin>>filename;
            //int table_id = open(filename, BPLUSTREE);
            int table_id = open(filename, SKIPLIST);
            std::cout<<"open result : "<<table_id<<std::endl;
            tables.push_back(table_id);
        }else if(cmd=="close"){
            int table_id;
            std::cin>>table_id;
            std::cout<<"close result : "<<close(table_id)<<std::endl;
        }else if(cmd=="quit"){
            end_db();
            std::cout<<"bye!"<<std::endl;
            return 0;
        }
    }
}