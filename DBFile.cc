#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "Heap.h"
#include "Sorted.h"
#include <iostream>
#include <fstream>

// stub file .. replace it with your own DBFile.cc
using namespace std;

DBFile *baseDbFile;

DBFile::DBFile (){
    
}

DBFile::~DBFile(){
    if(baseDbFile){
    delete baseDbFile;
    baseDbFile=NULL;
    }
}


int DBFile::Create (char *f_path, fType f_type, void *startup) {

    //Phase-2
    //Create a base db file object based on the file type
    if(f_type==heap){
        baseDbFile=new Heap();
    }
    else if(f_type==sorted){
        baseDbFile=new Sorted();
        
    }
    
//    //Check if baseDbFile exists
//    if(!baseDbFile){
//        cout<<"Not enough memory.Exit now."<<endl;
//        exit(1);
//    }
    
    return baseDbFile->Create(f_path,f_type,startup);
    

}

//Load the schema
void DBFile::Load (Schema &f_schema, char *loadpath) {
    //Phase-2
    baseDbFile->Load(f_schema,loadpath);
    
}
//Opens the given file
int DBFile::Open (char *f_path) {
    
    //Phase-2
    if(!baseDbFile){
        struct stat fileBuffer;
        int fileStatus;
        string metadataFilePath=f_path;
        metadataFilePath=metadataFilePath+".metadata";
        fileStatus=stat(metadataFilePath.c_str(), &fileBuffer);
        //Check if file exists
        if(fileStatus!=0){
            cout<<"File: "<<metadataFilePath<<" does not exist"<<endl;
            return 0;
        }
        ifstream inputFile;
        inputFile.open(metadataFilePath.c_str());
        string line;
        //Get the file type from metadata
        while(!inputFile.eof()){
            getline(inputFile,line);
            if(line.compare("heap")==0){
                baseDbFile=new Heap();
                break;
            }
            else if(line.compare("sorted")==0){
                baseDbFile=new Sorted();
                break;
            }
        }
    }
        return baseDbFile->Open(f_path);
}

//Moves the pointer to point to first record
void DBFile::MoveFirst () {
    //Phase-2
    baseDbFile->MoveFirst();
    
}
//Closes the file
int DBFile::Close () {
    //Phase-2
    return baseDbFile->Close();

}
/* Not used for now */
//void DBFile::initializePage(){
//
//    //baseDbFile->initializePage();
//    
//    
//    
//    
//	/*Since page is created just now, it does not represent any page of a file in the memory.
//	Hence currentPageNumber is 0.
//	Also currently nothing is written on the page, hence it is not dirty*/
//	this->currentPageNumber = 0;
//	this->isPageDirty = false;
//
//	cout<<"Buffer instance is initialized successfully."<<endl;
//
//}


/*This function is used to add records to the page and in turn to the database file*/
void DBFile::Add (Record &rec) {
    //Phase-2
    baseDbFile->Add(rec);

}

/* Not used for now */
///*This function writes a page to the disk ONLY if it is dirty*/
//void DBFile :: WritePageToFileIfDirty(Page* page, int whichPage){
//    
//    //Phase-2
//   // baseDbFile->WritePageToFileIfDirty(page,whichPage);
//    
//
//    /*Write dirty page to the file*/
//    if(this->isPageDirty){
//        
//        this->file.AddPage(page, whichPage);
//        
//        this->page.EmptyItOut();
//    }
//    
//    
//}

//Gets the next record
int DBFile::GetNext (Record &fetchme) {
    //Phase-2
   return baseDbFile->GetNext(fetchme);
    
}

                
//Applies CNF expression and gets the next record
int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    //Phase-2
    return baseDbFile->GetNext(fetchme,cnf,literal);
}
