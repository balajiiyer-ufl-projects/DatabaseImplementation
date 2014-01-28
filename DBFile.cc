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
    this->isPageDirty=false;
    this->currentPageNumber=1;
    return 1;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
    
    FILE *fileToLoad = fopen(loadpath, "r");
    
    if(!fileToLoad){
        cout<< "Can't open file name :" + string(loadpath);
    }
    
    Record record;
    while(record.SuckNextRecord(&f_schema,fileToLoad)==1){
        Add(record);
        
    }
    
}

int DBFile::Open (char *f_path) {
    file.Open(1, f_path);
    if(this->file.GetFileStatus()<0)
        return 0;
    cout<<"File opened successfully"<<endl;
    this->isPageDirty=false;
    this->currentPageNumber=1;
    return 1;
    
}

void DBFile::MoveFirst () {
    
    cout<<"Moving first"<<endl;
    this->currentPageNumber=1;
    cout<<"Before : Page records: "<<this->page.GetNumRecs()<<endl;
    this->file.GetPage(&this->page,1);
    cout<<"After : Page records: "<<this->page.GetNumRecs()<<endl;

}

int DBFile::Close () {
    WritePageToFileIfDirty(&this->page, this->currentPageNumber);
    return this->file.Close();
}

void DBFile :: WritePageToFileIfDirty(Page* page, int whichPage){
    
    
    /*Write dirty page to the file*/
    if(this->isPageDirty){
        
        this->file.AddPage(page, whichPage);
        
        this->page.EmptyItOut();
    }
    
    
}


void DBFile::Add (Record &rec) {

    
    //if(!this->isFileOpen){
//        Page p ;
//        this->page = p;
//        this->currentPageNumber = 1;
//        this->isFileOpen = true;
        
    //}
    
    
    //Try to append it to the existing page
    if(!this->page.Append(&rec)){
        
        cout << "Page is full.Appending the record to next page."<<endl;
        
        //this->page.PrintAllRecords(rec);
        //this->currentPageNumber++;
        WritePageToFileIfDirty(&this->page, this->currentPageNumber);
        this->currentPageNumber++;
        
        /*
         Now empty the page and load further records
         */
        
        this->page.Append(&rec);
        this->isPageDirty = true;
        
    }
    else this->isPageDirty=true;
    
    cout<<"Add: Page records: "<<this->page.GetNumRecs()<<endl;

}

int DBFile::GetNext (Record &fetchme) {
    cout << "Getting next record"<<endl;
    
    cout << "Getting next record"<<endl;
    
    /*For each page, perform getPage to bring the page into the buffer.
     Then perform GetFirst() to get records from the page.
     The moment GetFirst returns 0 means scanning of that page is completed.
     So fetch next page i.e. continue the loop.
     */
    
    int noOfPagesInFile = (this->file.GetLength()) - 1;
    /*Decrementing the length by 1 because first page of the file
     does not contain the data*/
    
    cout<< "File length is :: " << noOfPagesInFile <<endl;
    if(this->page.GetFirst (&fetchme)){
        cout << "Should print records" <<endl;
        return 1;
    }else{
        
        /*This means that we have scanned all the records on that page
         So we should move to next page
         */
        this->page.EmptyItOut();
        this->currentPageNumber++;
        
        //                cout << "Current Page number is " <<  this->currentPageNumber << endl;
        //                cout << "No of pages in file is " <<  noOfPagesInFile << endl;
        
        if(this->currentPageNumber < noOfPagesInFile){
            this->file.GetPage(&this->page, this->currentPageNumber);
            if(this->page.GetFirst(&fetchme)){
                return 1;
            }
            else{
                //FIXME this case should never occur ideally
                return 0;
            }
        }else{
            /*This means that we have scanned the file till the end*/
            return 0;
        }
        
    }
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
