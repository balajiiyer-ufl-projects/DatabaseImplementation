#include "BigQ.h"

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    
    // read data from in pipe sort them into runlen pages
    void *ret;
    //init data structures
    inPipe = &in;
    outPipe = &out;
    sortOrder = &sortorder;
    runLength=runlen;
    
    //Creating new thread.Generates runs
    pthread_t sortingThread;
    //Need to check this
    pthread_create(&sortingThread,NULL,&GenerateRuns,(void*)this);
    
    // construct priority queue over sorted runs and dump sorted data
 	// into the out pipe
    
    pthread_join(sortingThread, &ret);
    // finally shut down the out pipe
	out.ShutDown ();
}



BigQ::~BigQ () {
}

void* BigQ::GenerateRuns(void *ptr){
    
    ((BigQ*)ptr)->CreateSortedRuns();
    
    //FIX ME : Need to comment and test (San)
    return 0;
}

void* BigQ::CreateSortedRuns(){

	cout << "Inside create sorted runs" <<endl;
    fileName="runFile.bin"; //probably requires timeStamp;
    
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
		cout << "Page is completed in the run"<< endl;
            pageCountPerRun++;
            //pageCountPerRun is required to compare with runlength
            if(runLength==pageCountPerRun){
		cout << "One run is complete" <<endl;
                Page page;
                //Store the page count per run
                runIndices.push_back(pageCountPerRun);
                //Store the sort records in page
                
                while(queue.size()!=0){
                    if(!page.Append(queue.top())){
                        pagesTotal++;
			cout << pagesTotal << "th page is written to file part 1" << endl;
                        file.AddPage(&page,pagesTotal);
                        page.EmptyItOut();
                        page.Append(queue.top());
                    }
                    queue.pop();
                    
                }
                //Add the last page to file
                pagesTotal++;
		cout << pagesTotal << "th page is written to file part 2" << endl;
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
		cout << pagesTotal << "th page is written to file part 3" << endl;
                file.AddPage(&page,pagesTotal);
                page.EmptyItOut();
                page.Append(queue.top());
            }
            queue.pop();
        }
        //Add the last page to file
        pageCountPerRun++;
        pagesTotal++;
	cout << pagesTotal << "th page is written to file part 4" << endl;
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
    //outPipe->ShutDown();
    return 0;
}
int BigQ::MergeRuns(){
    
    file.Open(1, fileName);
    struct PQRecStruct *pqRec;
    RecordStruct record;
    int numOfRuns = runIndices.size();
    Run *runArray = new Run[numOfRuns];
    priority_queue<RecordStruct *, vector<RecordStruct *>, ComparePQ> mergeQueue(sortOrder);
    
    runArray[0].currentPageNumber = 1;
    runArray[0].totalPages = runIndices[0];
    file.GetPage(&(runArray[0].currentPage), runArray[0].currentPageNumber);
    /*Initialize Run data structure*/
    for(int i = 1; i< numOfRuns; i++){
        
        runArray[i].currentPageNumber = runIndices[i-1] + 1;
	cout << "Initially we push " << runArray[i].currentPageNumber << " th page for run number " << i << endl;
        runArray[i].totalPages = runIndices[i];
        file.GetPage(&(runArray[i].currentPage), runArray[i].currentPageNumber);
        
    }
    
    //Fetch the record and store it
    for(int runCount=0;runCount<numOfRuns;runCount++){
        runArray[runCount].currentPage.GetFirst(&record.record);
        //Insert the record into priority queue
        mergeQueue.push(&record);
        record.run_num=runCount;
    }
    
    //
    while(numOfRuns>0){
        
        //Fetch the record from priority queue
        RecordStruct candidate=*mergeQueue.top();
        //Insert each record into output queue
        outPipe->Insert(&(candidate.record));
        //Delete the record from priority queue
        mergeQueue.pop();
        //Get the next record from the run
        RecordStruct nextCandidate;
        nextCandidate=GetNextRecordFromRun(candidate.run_num,runArray);
        if(&nextCandidate==NULL){
            numOfRuns--;
            //Delete the run if exhausted
            delete &runArray[candidate.run_num];
        }
        else{
            //Add the next record to queue
            mergeQueue.push(&nextCandidate);
        }
        
    }
    
    
    // Check if all runs exhausted, exit
    file.Close();
    delete &runIndices;
    //outPipe->ShutDown();
    cout<<"totPages: "<<pagesTotal;
    return 1;
}


RecordStruct BigQ::GetNextRecordFromRun(int currentRunNumber,Run *array){
    RecordStruct nextRecord;
    if(array[currentRunNumber].currentPage.GetFirst(&nextRecord.record)){
        return nextRecord;
    
    }
    //Check for more pages in the run
    else{
        //Decrement the number of pages in the run
	cout << "Total pages to be consumed are "<< array[currentRunNumber].totalPages << " and current page number is " << array[currentRunNumber].currentPageNumber << " for run no " << currentRunNumber <<endl;
        array[currentRunNumber].totalPages--;
        if(array[currentRunNumber].totalPages>0){
        //Increment the page number
        array[currentRunNumber].currentPageNumber++;
        //Get the next Page and set as the current page
        file.GetPage(&(array[currentRunNumber].currentPage), array[currentRunNumber].currentPageNumber);
         //Return the next record
            array[currentRunNumber].currentPage.GetFirst(&nextRecord.record);
        }
        else{
		cout << "Case where run no " << currentRunNumber << "got exhausted " <<endl;
            delete &nextRecord;
        }
    }
    
    return nextRecord;
    
    //return 1;
}
