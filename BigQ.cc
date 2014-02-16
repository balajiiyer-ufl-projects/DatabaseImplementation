#include "BigQ.h"

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    
    // read data from in pipe sort them into runlen pages
    
    //init data structures
    inPipe = &in;
    outPipe = &out;
    sortOrder = &sortorder;
    runLength=runlen;
    
    //Creating new thread.Generates runs
    pthread_t sortingThread;
    //Need to check this
    pthread_create(&sortingThread,NULL,SortPhase1,(void*)this);
    
    // construct priority queue over sorted runs and dump sorted data
 	// into the out pipe
    
    // finally shut down the out pipe
	out.ShutDown ();
}



BigQ::~BigQ () {
}

void* BigQ::SortPhase1(void *ptr){
    
    ((BigQ*)ptr)->CreateSortedRuns();
    
    //FIX ME : Need to comment and test (San)
    return 0;
}

void* BigQ::CreateSortedRuns(){
    fileName="runFile.run"; //probably requires timeStamp;
    Page page;
    pagesTotal=0;
    pageCount=0;
    Record *record=new Record;
    Page pagesForRunlength;
    //Need to check syntax
    priority_queue<Record *, vector<Record *>, CompareRec> queue(sortOrder);
    file.Open(0,fileName);
    
    while(inPipe->Remove(record)){

        //page is required to compare with runlength
        if(!pagesForRunlength.Append(record)){
            pagesTotal++;
            if(runLength==pagesTotal){
                runIndices.push_back(pagesTotal);
                //Store the sort records in page
                
                while(queue.size()>0){
                    if(!page.Append(queue.top())){
                        pageCount++;
                        file.AddPage(&page,pageCount);
                        page.EmptyItOut();
                        page.Append(queue.top());
                    }
                    queue.pop();
                    
                }
                
                //Reset the count for next run
                pagesTotal=0;
                pageCount=0;
            }
            pagesForRunlength.EmptyItOut();
            pagesForRunlength.Append(record);
            queue.push(record);
            
        }
    
    }
    while(queue.size()>0){
        if(!page.Append(queue.top())){
            pageCount++;
            file.AddPage(&page,pageCount);
            page.EmptyItOut();
            page.Append(queue.top());
        }
        queue.pop();
    }
    //Delete objects and close the file
    pagesForRunlength;
    delete record;
    file.Close();
    //FIX ME : Need to comment and test (San)
    return 0;
    }