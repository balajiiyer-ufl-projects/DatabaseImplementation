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
    RecordStruct *record = new RecordStruct;
    int numOfRuns = runIndices.size();
    Run *runArray = new Run[numOfRuns];
    priority_queue<RecordStruct *, vector<RecordStruct *>, ComparePQ> mergeQueue(sortOrder);
    
    runArray[0].currentPageNumber = 1;
    runArray[0].totalPages = runIndices[0];
    file.GetPage(&(runArray[0].currentPage), runArray[0].currentPageNumber);
    /*Initialize Run data structure*/
    for(int i = 1; i< numOfRuns; i++){
<<<<<<< HEAD
        runArray[i].currentPageNumber = (runIndices[i-1])*(i-1) + 1;
=======
        runArray[i].currentPageNumber = (runIndices[i-1]) * (i-1) + 1;
>>>>>>> 0f6933f4d0a22afda5a63d2384b1932f5944ab20
	cout << "Initially we push " << runArray[i].currentPageNumber << " th page for run number " << i << endl;
        runArray[i].totalPages = runIndices[i];
        file.GetPage(&(runArray[i].currentPage), runArray[i].currentPageNumber);
        
    }
    
    //Fetch the record and store it
    for(int runCount=0;runCount<numOfRuns;runCount++){
        runArray[runCount].currentPage.GetFirst(&(record->record));
        //Insert the record into priority queue
        mergeQueue.push(record);
        record->run_num=runCount;
    }
    
    //
    while(numOfRuns>0){
	cout << "Before you pop the record from merge queue" << endl;        
        //Fetch the record from priority queue
        RecordStruct *candidate=mergeQueue.top();
        //Insert each record into output queue
        outPipe->Insert(&(candidate->record));
        //Delete the record from priority queue
        mergeQueue.pop();
	cout << "After you pop the record from merge queue" << endl;
        //Get the next record from the run
<<<<<<< HEAD
        RecordStruct *nextCandidate=new RecordStruct;
        if(!GetNextRecordFromRun(candidate->run_num,runArray,nextCandidate))
        {
=======
        RecordStruct *nextCandidate;
        nextCandidate=GetNextRecordFromRun(candidate->run_num,runArray);
        cout<<&nextCandidate<<endl;
        if(!nextCandidate){
		cout << "inside if " <<endl;
>>>>>>> 0f6933f4d0a22afda5a63d2384b1932f5944ab20
            numOfRuns--;
            //Delete the run if exhausted
            delete (&runArray[candidate->run_num]);
        }
        else{
		cout << "inside else" <<endl;
            //Add the next record to queue
            mergeQueue.push(nextCandidate);
		 cout << "inside else after" <<endl;
        }
        
    }
    
   cout << "While loop ends" <<endl; 
    // Check if all runs exhausted, exit
    file.Close();
    delete &runIndices;
    //outPipe->ShutDown();
    cout<<"totPages: "<<pagesTotal;
    return 1;
}


bool BigQ::GetNextRecordFromRun(int currentRunNumber,Run *array,RecordStruct *nextRecord){
    //RecordStruct *nextRecord=new RecordStruct;
    int status;
    if(array[currentRunNumber].currentPage.GetFirst(&(nextRecord->record))){
<<<<<<< HEAD
        status=1;
=======
	cout <<"Regular case" << endl;
        return nextRecord;
>>>>>>> 0f6933f4d0a22afda5a63d2384b1932f5944ab20
    
    }
    //Check for more pages in the run
    else{
	cout << "Here 1" <<endl;
        //Decrement the number of pages in the run
	cout << "Total pages to be consumed are "<< array[currentRunNumber].totalPages << " and current page number is " << array[currentRunNumber].currentPageNumber << " for run no " << currentRunNumber <<endl;
        array[currentRunNumber].totalPages--;
        if(array[currentRunNumber].totalPages>0){
        //Increment the page number
        array[currentRunNumber].currentPageNumber++;
        //Get the next Page and set as the current page
        file.GetPage(&(array[currentRunNumber].currentPage), array[currentRunNumber].currentPageNumber);
         //Return the next record
            array[currentRunNumber].currentPage.GetFirst(&(nextRecord->record));
            status=1;
        }
        else{	cout << "here 2" <<endl;
		cout << "Case where run no " << currentRunNumber << "got exhausted " <<endl;
<<<<<<< HEAD
            //delete nextRecord;
            status=0;
=======
            delete nextRecord;
>>>>>>> 0f6933f4d0a22afda5a63d2384b1932f5944ab20
        }
    }
    
    return status;

}
