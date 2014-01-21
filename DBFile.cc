#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

#include <iostream>

// stub file .. replace it with your own DBFile.cc
using namespace std;

DBFile *dbfile;


DBFile::DBFile () {
    
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
    
    this->file.Open(0, f_path);
    
   
    
    if(this->file.GetFileStatus()<0)
        return 0;
    
    return 1;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
    
    FILE *fileToLoad = fopen(loadpath, "r");
   
    if(!fileToLoad){
        cout<< "Can't open file name :" + string(loadpath);
    }

//    /*Write dirty page to the file*/
//    if(this->isPageDirty){	    
//    	
//	WritePageToFile(&(this->page), this->currentPageNumber++);
//
//	this->page.EmptyItOut();
//    }

    WritePageToFileIfDirty(&(this->page), this->currentPageNumber++);
    Record record;
    while(record.SuckNextRecord(&f_schema,fileToLoad)){
	//Changed method declaration to accept schema
	//This adds records to the page
        Add(record);
    }

    cout<<"Records loaded successfully"<<endl;
    WritePageToFileIfDirty(&(this->page), this->currentPageNumber++);
    //fclose(fileToLoad);
}

int DBFile::Open (char *f_path) {
    
    file.Open(1, f_path);
    if(this->file.GetFileStatus()<0)
        return 0;
    cout<<"File opened successfully"<<endl;
    return 1;
    
    
}

void DBFile::MoveFirst () {
    
    cout<<"Moving first"<<endl;
    this->currentPageNumber=2;
    cout<<"Before : Page records: "<<this->page.GetNumRecs()<<endl;
    this->file.GetPage(&this->page,2);
    cout<<"After : Page records: "<<this->page.GetNumRecs()<<endl;
    //this->currentPageNumber++;
    
}

int DBFile::Close () {
    
  WritePageToFileIfDirty(&this->page, this->currentPageNumber);
    return this->file.Close();
}

void DBFile::Add (Record &rec) {

	if(!this->isFileOpen){
        Page p ;
		this->page = p;
		this->currentPageNumber = 1;
		this->isFileOpen = true;
	
	}
	
	
		//Try to append it to the existing page
		if(!this->page.Append(&rec)){
			
			cout << "Page is full.Appending the record to next page."<<endl;

			//this->page.PrintAllRecords(rec);
			//this->currentPageNumber++;
            WritePageToFileIfDirty(&this->page, this->currentPageNumber++);

			/*
			Now empty the page and load further records
			*/
			
			this->page.Append(&rec);
			this->isPageDirty = true;
						
		}
			
    cout<<"Add: Page records: "<<this->page.GetNumRecs()<<endl;

    
}
//void DBFile :: WritePageToFile(Page* page, int whichPage){
//	
//	this->file.AddPage(page, whichPage);
//
//}

void DBFile :: WritePageToFileIfDirty(Page* page, int whichPage){
	
    
	/*Write dirty page to the file*/
    if(this->isPageDirty){
    	
        this->file.AddPage(page, whichPage);
        
        this->page.EmptyItOut();
    }

    
}

int DBFile::GetNext (Record &fetchme) {
    /*cout<<"Getting next record"<<endl;
    fetchme = *(this->page.GetCurrent());
    if(&fetchme==NULL)
        return 0;
    cout<<"Records received successfully"<<endl;
    return 1;
     */
    
    cout << "Getting next record"<<endl;
    
    if (this->currentPageNumber == 0)
    {
        file.GetPage(&this->page,this->currentPageNumber++);
    }

    
    if(!this->page.GetFirst(&fetchme))
    {
        if(this->currentPageNumber<this->file.GetLength()-1){
            this->page.EmptyItOut();
        this->currentPageNumber++;
        this->file.GetPage(&this->page,this->currentPageNumber);
            if(this->page.GetFirst(&fetchme))
                return 1;
            else {
                cout<<"Coudln't fetch the record"<<endl;
             return 0;
            }
        }
    }
    
    return 1;
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine comp;
    while(GetNext(fetchme)){
        if(comp.Compare(&fetchme, &literal, &cnf)) {
            return 1;
        }
    }
    return 0;
}
