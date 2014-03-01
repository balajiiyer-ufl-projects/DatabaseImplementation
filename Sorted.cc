#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "Sorted.h"
#include <iostream>
#include <fstream>
#include <stdio.h>


//Just a simple copy of Heap.cc.
//Need to change the datastructure
using namespace std;


Sorted::Sorted () {
    int buffsz = 100; // pipe cache size
    numRecsInPipe=0;
    readingMode=false;
    Pipe input (buffsz);
	Pipe output (buffsz);
    //inPipe = new Pipe(buffSize);
    //outPipe = new Pipe (buffSize);
    inPipe=&input;
    outPipe=&output;
}

Sorted::~Sorted(){
    
}

//Create Done
int Sorted::Create (char *f_path, void *startup) {
    cout<<"Sorted:Saving Metadata"<<endl;
    //Save metadata file path name
    metadataFile=f_path;
    
    /*This is needed because when we move data from merged file,
     we need original file path*/
    fileName = f_path;
    sortInfo=(SortInfo*)startup;
    
    this->file.Open(0, f_path);
    
    if(this->file.GetFileStatus()<0)
        return 0;
    this->isPageDirty=false;
    this->currentPageNumber=1;
    
    
    
    WriteMetadata();
    return 1;
}

//Load the schema
void Sorted::Load (Schema &f_schema, char *loadpath) {
    
    //Set writing mode to true i.e. reading mode is false
    if(readingMode)
        readingMode=false;
    
    //Open the file
    FILE *fileToLoad = fopen(loadpath, "r");
    if(!fileToLoad){
        cout<< "Can't open file name :" + string(loadpath);
    }
    
    Record record;
    //Load the schema and add each record
    while(record.SuckNextRecord(&f_schema,fileToLoad)==1){
        readingMode = false;
        Add(record);
        
    }
}

//Open Done
//Open the file
int Sorted::Open (char *f_path) {
    
    cout<<"Sorted:Opening a sorted file"<<endl;
    string fileType;
    string type;
    string metadataFilePath=f_path;
    int numberOfAttributes;
    metadataFilePath=metadataFilePath+".metadata";
    ifstream readMetadata;
    
    readMetadata.open(metadataFilePath.c_str());
    if(!sortInfo){
        sortInfo=new SortInfo();
        sortInfo->myOrder=new OrderMaker();
        
        readMetadata>>fileType;
        readMetadata>>sortInfo->runLength;
        readMetadata>>numberOfAttributes;
        int whichAtts[MAX_ANDS];
        
        Type whichTypes[MAX_ANDS];
        for(int count=0;count<numberOfAttributes;count++){
            //Set which attribute;
            readMetadata>>whichAtts[count];
            readMetadata>>type;
            if(type.compare("Int")==0){
                whichTypes[count]=Int;
            }
            else if(type.compare("Double")==0){
                whichTypes[count]=Double;
            }
            else
                whichTypes[count]=String;
            
        }
        //Set numberOfAttributes whichAtts and whichTypes
        sortInfo->myOrder->SetAttributeMetadata(numberOfAttributes,whichAtts,whichTypes);
        
    }
    
    this->file.Open(1, f_path);
    if(this->file.GetFileStatus()<0)
        return 0;
    
    cout<<"File opened successfully"<<endl;
    
    //Initialize parameters
    this->isPageDirty=false;
    this->currentPageNumber=1;
    
    return 1;
    
}

void Sorted::MoveFirst () {
    /*Since we are moving to the first page, page number is 1*/
    MergeRecordsWithSortedFile();
    file.GetLength();
    this->file.GetPage(&this->page, 0);
    this->currentPageNumber = 1;
}

int Sorted::Close () {
    MergeRecordsWithSortedFile();
    //readingMode=true;
    //WritePageToFileIfDirty(&this->page, this->currentPageNumber);
    return this->file.Close();
}

void Sorted::Add (Record &rec) {
    
    /**
     initialize the big queue instance
     */
    readingMode=false;
    int buffSize = 100;
    
    
	if(numRecsInPipe/100000 == 1){
		
        /*The input pipe is full and we need to
         merge the records into sorted file*/
        inPipe->ShutDown();
        
        MergeRecordsWithSortedFile();
        
        /*Reset the num recs in Pipe*/
        numRecsInPipe = 0;
        
        /*Open the pipe again*/
        inPipe->Open();
        
	}else{
        inPipe->Insert(&rec);
        numRecsInPipe++;
    }
    
}

void Sorted :: MergeRecordsWithSortedFile(){
    
	/*This is going to maintain the newly inputed records in the sorted order*/
    
	bigQ = new BigQ(*inPipe, *outPipe, *sortInfo->myOrder, sortInfo->runLength);
    
	/*This merge queue merges the records from already sorted file with newly added records.
     And sort order should be same as that of the sorted file*/
	priority_queue<RecordStructForSorted *, vector<RecordStructForSorted *>, ComparePQSorted> mergeQueue(&(*sortInfo->myOrder));
    
    /*This file is created just for merging.*/
    char *mergeFileName="mergedFile.bin";
    
    
    int pagesTotal=0;
    
    /*Check if the main file exists*/
    if(file.GetLength() != 0){
        
        pagesTotal=file.GetLength()-1;
        mergeFile.Open(0, mergeFileName);
        RecordStructForSorted *pipeRecStruct, *fileRecStruct,*nextRecStruct;
        Record *pipeRec = new Record;
        Record *fileRec = new Record;
        Record *nextRec = new Record;
        
        /*Get initial record from output pipe and put into merge queue*/
        if(outPipe->Remove(pipeRec)){
            pipeRecStruct = new RecordStructForSorted;
            pipeRecStruct->record = *pipeRec;
            pipeRecStruct->sourceName = 0;
            /*Insert it into the queue*/
            mergeQueue.push(pipeRecStruct);
        }
        /*Get initial record from sorted file and put into merge queue*/
        file.GetPage(&page, 0);
        
        if(page.GetFirst(fileRec)){
            fileRecStruct = new RecordStructForSorted;
            fileRecStruct->record = *fileRec;
            fileRecStruct->sourceName = 1;
	        /*Insert it into the queue*/
            mergeQueue.push(fileRecStruct);
        }
        
        while(!mergeQueue.empty())
        {
            //Fetch the record from priority queue
            RecordStructForSorted *candidate=new RecordStructForSorted;
            candidate=mergeQueue.top();
            bool sourceCandidate = candidate->sourceName;
            
            //Delete the record from priority queue
            mergeQueue.pop();
            
            /*insert the record into the merge file using similar function as heap*/
            InsertRecordIntoFile(candidate->record);
            
            /*Pull the next record from the same source*/
            nextRecStruct = new RecordStructForSorted;
            if(!sourceCandidate){
                
                if(outPipe->Remove(nextRec)){
                    nextRecStruct->sourceName = 0;
                    nextRecStruct->record = *nextRec;
                    /*Insert it into the queue*/
                    mergeQueue.push(nextRecStruct);
                }
                
            }else{
                /*Get next record from file into nextRec*/
                if(GetNext(*nextRec)){
                    
                    nextRecStruct->sourceName = 1;
                    nextRecStruct->record = *nextRec;
                    /*Insert it into the queue*/
                    mergeQueue.push(nextRecStruct);
                }
                
            }
            
        }//End of while
        //delete pipeRec;
        //delete fileRec;
        //delete nextRec;
        
//        std::ifstream  src("mergedFile.bin", std::ios::binary);
//        std::ofstream  dst(fileName,std::ios::binary);
//        
//        dst << src.rdbuf();
//        
//        remove(mergeFileName);
        
        //        /*copy the data to original location.
        //         Delete merge file when job is done*/
        //        if(co(mergeFileName,fileName)==0){
        //            cout<<"Could not rename the file"<<endl;
        //        }
        //        else{
        //
        //        }
        //        //system(string("mv mergedFile.bin "+fileName).c_str());
        
        
    }else{
        Record *rec = new Record;
        
        while(outPipe->Remove(rec)){
            
            /*insert the record into the sorted file using similar
             function as heap. We need not create the merge file,
             since there is no original sorted file*/
            InsertRecordIntoFile(*rec);
            rec = new Record;
        }
        
    }
    //Should it be here?
    std::ifstream  src("mergedFile.bin", std::ios::binary);
    std::ofstream  dst(fileName,std::ios::binary);
    
    dst << src.rdbuf();
    
    remove(mergeFileName);
    
}

int Sorted::GetNext (Record &fetchme) {
    /*For each page, perform getPage to bring the page into the buffer.
     Then perform GetFirst() to get records from the page.
     The moment GetFirst returns 0 means scanning of that page is completed.
     So fetch next page i.e. continue the loop.
     */
    if(!readingMode){
        MergeRecordsWithSortedFile();
        readingMode=true;
    }
    int noOfPagesInFile = (this->file.GetLength()) - 1;
    /*Decrementing the length by 1 because first page of the file
     does not contain the data*/
    
    //	cout<< "File length is :: " << noOfPagesInFile <<endl;
    if(this->page.GetFirst (&fetchme)){
        //		cout << "Should print records" <<endl;
        return 1;
    }else{
        
        /*This means that we have scanned all the records on that page
         So we should move to next page
         */
        this->page.EmptyItOut();
        //this->currentPageNumber++;
        
        //		cout << "Current Page number is " <<  this->currentPageNumber << endl;
        //		cout << "No of pages in file is " <<  noOfPagesInFile << endl;
        
        if(this->currentPageNumber < noOfPagesInFile){
            this->file.GetPage(&this->page, this->currentPageNumber);
            this->currentPageNumber++;
            if(this->page.GetFirst(&fetchme)){
                return 1;
            }
            else{
                //this case should never occur ideally
                return 0;
            }
        }else{
            /*This means that we have scanned the file till the end*/
            return 0;
        }
        
    }
}


int Sorted::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine comp;
    
    while(GetNext(fetchme)){
        if(comp.Compare(&fetchme, &literal, &cnf)) {
            return 1;
        }
    }
    return 0;
}

/*This function writes a page to the disk ONLY if it is dirty*/
void Sorted:: WritePageToFileIfDirty(Page* page){
    
    
    /*Write dirty page to the file*/
    if(this->isPageDirty){
        int whichPage = (int)file.GetLength() + 1;
        this->file.AddPage(page, whichPage);
        this->page.EmptyItOut();
    }
    
}
/*This function insert a new record into the file - heap add*/
void Sorted:: InsertRecordIntoFile(Record &rec){
    
    /*This function will take care of writing the page out
     if the page becomes full before adding a record.
     
     But it will NOT take care of writing the page out once all records are added and
     still page is not full. Calling function should take care of it*/
    
	/*Check if the page is full before adding a record*/

    int pagesTotal=mergeFile.GetLength()-1;
    if(pagesTotal==-1){
        pagesTotal=0;
    }
	if(!this->page.Append(&rec)){
        
        
        mergeFile.AddPage(&page,pagesTotal);
        pagesTotal++;
        page.EmptyItOut();
        
		//WritePageToFileIfDirty(&this->page);
		//cout << "Page is full.Appending the record to next page."<<endl;
        
		/*Here, File::GetPage function can't be called. This is because
         that function returns an existing page whereas here we are adding a new page.
         But WritePageToFile... takes care of cleaning up of page instance, hence we
         just have to append the records to the new page*/
		
		/*Now load further records*/
		this->page.Append(&rec);
		/*Since we added a record, we should set page as dirty*/
		//this->isPageDirty = true;
        
	}
    mergeFile.AddPage(&page, pagesTotal);
    //    else{
    //		//cout << "Records appended" << endl;
    //		/*This means record is appended so set page as dirty*/
    //		this->isPageDirty = true;
    //	}
    
    
}

void Sorted::WriteMetadata(){
    
    if(!GetMetadataFile().empty()){
        ofstream outputStream;
        outputStream.open(string(GetMetadataFile()
                                 +".metadata").c_str(),ios::trunc);
        
        outputStream<<"sorted\n";
        outputStream<<sortInfo->runLength<<"\n";
        outputStream<<sortInfo->myOrder->ToString();
        outputStream.close();
    }
    
}
