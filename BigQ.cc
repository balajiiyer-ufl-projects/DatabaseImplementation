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
    pthread_create(&sortingThread,NULL,&SortPhase1,(void*)this);
    
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
    
    pageCountPerRun=0;//Stores the page count per run
    pagesTotal=0;//Total number of pages in a file
    Record *record=new Record;
    Record *recordCopy;
    Page pagesForRunlength;
    //Need to check syntax
    priority_queue<Record *, vector<Record *>, CompareRec> queue(sortOrder);
    file.Open(0,fileName);
    
    while(inPipe->Remove(record)){
        recordCopy=new Record;
        //Copy the record since appending would consume the record
        recordCopy->Copy(record);
        
        queue.push(recordCopy);
        

        if(!pagesForRunlength.Append(record)){
            pageCountPerRun++;
            //pageCountPerRun is required to compare with runlength
            if(runLength==pageCountPerRun){
                Page page;
                //Store the page count per run
                runIndices.push_back(pageCountPerRun);
                //Store the sort records in page
                
                while(queue.size()!=0){
                    if(!page.Append(queue.top())){
                        pagesTotal++;
                        file.AddPage(&page,pagesTotal);
                        page.EmptyItOut();
                        page.Append(queue.top());
                    }
                    queue.pop();
                    
                }
                //Add the last page to file
                pagesTotal++;
                file.AddPage(&page,pagesTotal);
                page.EmptyItOut();
                queue.empty();
                //Reset the count for next run
                pageCountPerRun=0;

            }
            pagesForRunlength.EmptyItOut();
            pagesForRunlength.Append(record);
            
        }
        
    }
    //Delete record
    delete record;
    //If queue still contains record,append it to a new run
    if(queue.size()!=0){
        Page page;
        pageCountPerRun=0;
        while(queue.size()!=0){
            if(!page.Append(queue.top())){
                pageCountPerRun++;
                pagesTotal++;
                file.AddPage(&page,pagesTotal);
                page.EmptyItOut();
                page.Append(queue.top());
            }
            queue.pop();
        }
        //Add the last page to file
        pageCountPerRun++;
        pagesTotal++;
        file.AddPage(&page,pagesTotal);
        page.EmptyItOut();
        queue.empty();
        
    }
    runIndices.push_back(pageCountPerRun);
    for (int i = 0; i < runIndices.size(); i++) {
        cout << "runIndex[" << i << "]: " << runIndices[i] << "\n";
    }
    file.Close();
    MergeRuns();
    //FIX ME : Need to comment and test (San)
    return 0;
}
int BigQ::MergeRuns(){
    
    
    
    return 1;
}