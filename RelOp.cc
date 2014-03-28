#include "RelOp.h"
#define BUFF_SIZE 100

/**SelectFile**/
void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    inputFile = &inFile;
    outputPipe = &outPipe;
    cnf=selOp;
    recLiteral=literal;
    pthread_create(&thread,NULL,&GetDataFromFileToPipe,(void*)this);
    }

//Select File
void* SelectFile::GetDataFromFileToPipe(void *ptr){
    SelectFile *sf= ((SelectFile*)ptr);
    Record fetchme;

    while(sf->inputFile->GetNext(fetchme, sf->cnf, sf->recLiteral)){
        sf->outputPipe->Insert(&fetchme);
    }
    sf->outputPipe->ShutDown();
    }

void SelectFile::WaitUntilDone () {
	pthread_join (thread, NULL);
}

//Not used for now
void SelectFile::Use_n_Pages (int runlen) {

}

/**Select Pipe**/
void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
    inputPipe = &inPipe;
    outputPipe = &outPipe;
    cnf=selOp;
    recLiteral=literal;
    pthread_create(&thread,NULL,&GetDataFromInputToOutputPipe,(void*)this);
    
}


void* SelectPipe::GetDataFromInputToOutputPipe(void *ptr){
    SelectPipe *sp= ((SelectPipe*)ptr);
    Record fetchme;
    while(sp->inputPipe->Remove(&fetchme)){
        sp->outputPipe->Insert(&fetchme);
    }
    
    sp->outputPipe->ShutDown();
    
}

void SelectPipe::WaitUntilDone () {
	pthread_join (thread, NULL);
}

//Not used for now
void SelectPipe::Use_n_Pages (int runlen) {
    
}

/**Project**/
void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput){
    inputPipe = &inPipe;
    outputPipe = &outPipe;
    attToKeep=keepMe;
    attNumInput=numAttsInput;
    attNumOutput=numAttsOutput;
    pthread_create(&thread,NULL,&ProjectFromInputToOutputPipe,(void*)this);
    return;
}


void* Project::ProjectFromInputToOutputPipe(void *ptr){
    Project *p= ((Project*)ptr);
    Record fetchme;
    while(p->inputPipe->Remove(&fetchme)){
        fetchme.Project(p->attToKeep,p->attNumOutput,p->attNumInput);
        p->outputPipe->Insert(&fetchme);
    }
    //Shutdown pipes
    p->outputPipe->ShutDown();
    
}

void Project::WaitUntilDone () {
	pthread_join (thread, NULL);
}

//Not used for now
void Project::Use_n_Pages (int runlen) {
    
}

/**DuplicateRemoval**/
void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe,Schema &mySchema){
    inputPipe = &inPipe;
    outputPipe = &outPipe;
    schema=&mySchema;
    pthread_create(&thread,NULL,&RemoveDuplicates,(void*)this);
    return;
}



void* DuplicateRemoval::RemoveDuplicates(void *ptr){
    DuplicateRemoval *dr= ((DuplicateRemoval*)ptr);
    int numberOfAttributes=dr->schema->GetNumAtts();
    Attribute *attType=dr->schema->GetAtts();
    OrderMaker myOrder;
    int whichAtts[MAX_ANDS];
    Type whichTypes[MAX_ANDS];
    for(int count=0;count<numberOfAttributes;count++){
        //Set which attribute;
        whichAtts[count]=count;
        whichTypes[count]=attType[count].myType;
    }
    //Set numberOfAttributes whichAtts and whichTypes
    myOrder.SetAttributeMetadata(numberOfAttributes,whichAtts,whichTypes);
    
    Pipe *interimPipe=new Pipe(BUFF_SIZE);
    //For now run length is hardcoded
	BigQ bq (*dr->inputPipe,*interimPipe,myOrder,10); //dr->runLength);
    
    //Remove duplicates
    Record *newRecord;
    Record *oldRecord=new Record();
    ComparisonEngine compEng;
    //bool firstIteration=true;
    while(interimPipe->Remove(newRecord)){
//        if(firstIteration){
//            oldRecord->Copy(newRecord);
//            firstIteration=false;
//        }
        //If the records are not equal,add the new record
        //to output pipe and set the old record to new record
        if(compEng.Compare(oldRecord,newRecord, &myOrder)!=0){
            dr->outputPipe->Insert(newRecord);
            oldRecord->Copy(newRecord);
        }
    }
    interimPipe->ShutDown();
    dr->outputPipe->ShutDown();
    delete oldRecord;
}

void DuplicateRemoval::WaitUntilDone () {
	pthread_join (thread, NULL);
}

//Not used for now
void DuplicateRemoval::Use_n_Pages (int runlen) {
    runLength=runlen;
}


