#include "RelOp.h"
#include <fstream>


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
    delete interimPipe;
}

void DuplicateRemoval::WaitUntilDone () {
	pthread_join (thread, NULL);
}

//Not used for now
void DuplicateRemoval::Use_n_Pages (int runlen) {
    //runLength=runlen;
}

/*For sum operator*/
void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
    
	inputPipe = &inPipe;
	outputPipe = &outPipe;
	funcPtr = &computeMe;
	pthread_create(&thread,NULL,&GetSum,(void*)this);
	return;
}

void* Sum::GetSum(void *ptr){
    
	Sum *sumPtr = (Sum *) ptr;
	Record fetchme;
	Type t;
	int ival = 0;
	double dval = 0;
	double sum = 0;
	
	while(sumPtr->inputPipe->Remove(&fetchme)){
		t = sumPtr->funcPtr->Apply (fetchme, ival, dval);
		sum += (ival + dval);
	}
	
	Attribute DA = {(char*)"double", Double};
    Schema out_sch ((char*)"out_sch",1, &DA);
	
	ofstream myfile;
	myfile.open ("sumValue");
	myfile << sum << "|" ;
	myfile.close();
	FILE *tmpFile = fopen ("sumValue" , "r");

	Record *result=new Record;
	result->SuckNextRecord(&out_sch, tmpFile);
	
	sumPtr->outputPipe->Insert(result);
    fclose(tmpFile);
	//Shutdown pipe
	sumPtr->outputPipe->ShutDown();
    delete result;
    remove("sumValue");

}



void Sum::WaitUntilDone () {
    
	pthread_join (thread, NULL);
    
}
void Sum::Use_n_Pages (int n) { }


/**Group By **/


/*For sum operator*/
void GroupBy::Run (Pipe &inPipe, Pipe &outPipe,OrderMaker &groupAtts, Function &computeMe) {
    
	inputPipe = &inPipe;
	outputPipe = &outPipe;
	funcPtr = &computeMe;
    myOrder=groupAtts;
	pthread_create(&thread,NULL,&GroupAndSum,(void*)this);
	return;
}

/**
 1.Create a local pipe
 2.Create a BigQ instance
 3.Compare record from the pipe
 4.If equal,group by and calcluate sum
 5.If differnet push it to output pipe
 **/

void* GroupBy::GroupAndSum(void *ptr){
    
	GroupBy *grpBy = (GroupBy *) ptr;
	Record fetchme;
    Pipe *interimPipe=new Pipe(BUFF_SIZE);
    //For now run length is hardcoded
	BigQ bq (*grpBy->inputPipe,*interimPipe,grpBy->myOrder,10); //dr->runLength);
    
    //int *atts= grpBy->myOrder.GetAttributeArray();

    Record *newRecord=new Record;
    Record *oldRecord=new Record();
    ComparisonEngine compEng;
    bool firstIteration=true;
    
    //Sum parameters
	double sum = 0;
    
    
    while(interimPipe->Remove(newRecord)){
        if(firstIteration){
            oldRecord->Copy(newRecord);
            firstIteration=false;
        }
        //If the records are equal,sum the records
        if(compEng.Compare(oldRecord,newRecord, &grpBy->myOrder)==0){
            int ival = 0;
            double dval = 0;

            grpBy->funcPtr->Apply (*newRecord, ival, dval);
            sum += (ival + dval);
            //delete record
            delete newRecord;
            newRecord=NULL;
            
        }
        else{
            //oldRecord->Project(atts,grpBy->myOrder.GetNumberOfAttributes(),grpBy->myOrder.GetNumberOfAttributes());
            
            //Schema out_sch ((char*)"out_sch",1,grpBy->myOrder.GetCurrentSchema().GetAtts());
            Attribute DA = {(char*)"double", Double};
            Schema out_sch ((char*)"out_sch",1, &DA);
            
            ofstream myfile;
            myfile.open ("sumValue");
            myfile << sum << "|";// << oldRecord;
            myfile.close();
            FILE *tmpFile = fopen ("sumValue" , "r");
            Record *result=new Record;
            result->SuckNextRecord(&out_sch, tmpFile);
            fclose(tmpFile);
            
            
            grpBy->outputPipe->Insert(result);
            
            oldRecord->Copy(newRecord);
            
            //CULPRIT :Need to calculate the sum of last record if any
            if(newRecord!=NULL)
            {
                int ival = 0; double dval = 0;
                grpBy->funcPtr->Apply(*newRecord, ival, dval);
                sum = (ival + dval);
                delete newRecord;
                newRecord= NULL;
            }
            remove("sumValue");
            delete result;
        }
    }
    //Adding the last record
    //oldRecord->Project(atts,grpBy->myOrder.GetNumberOfAttributes(),grpBy->myOrder.GetNumberOfAttributes());
    
    //Schema out_sch ((char*)"out_sch",1,grpBy->myOrder.GetCurrentSchema().GetAtts());
    Attribute DA = {(char*)"double", Double};
    Schema out_sch ((char*)"out_sch",1, &DA);
    ofstream myfile;
    myfile.open ("sumValue");
    myfile << sum << "|";// << oldRecord;
    myfile.close();
    FILE *tmpFile = fopen ("sumValue" , "r");
    Record *result=new Record;
    result->SuckNextRecord(&out_sch, tmpFile);
    
    grpBy->outputPipe->Insert(result);
    
    interimPipe->ShutDown();
    grpBy->outputPipe->ShutDown();
    delete result;
    delete oldRecord;
    delete interimPipe;
    remove("sumValue");
}



void GroupBy::WaitUntilDone () {
    
	pthread_join (thread, NULL);
    
}
void GroupBy::Use_n_Pages (int n) { }


void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    inputPipeL=&inPipeL;
    inputPipeR=&inPipeR;
    outputPipe=&outPipe;
    cnf=selOp;
    recLiteral=literal;
    pthread_create(&thread,NULL,&JoinRecords,(void*)this);
	return;
    
    
}
void Join::WaitUntilDone(){
    
}
void Join::Use_n_Pages (int n) { }

void* Join::JoinRecords(void *ptr){
    Join* joinPtr=(Join*)ptr;
    
    OrderMaker oLeft,oRight;
    
    joinPtr->cnf.GetSortOrders(oLeft, oRight);
    
    if(oLeft.GetNumberOfAttributes()==oRight.GetNumberOfAttributes() && oLeft.GetNumberOfAttributes()>0){
        
        Pipe *leftInterimPipe=new Pipe(BUFF_SIZE);
        Pipe *rightInterimPipe=new Pipe(BUFF_SIZE);
        
        /*Sort records from both the pipes since the ordermaker matches*/
        
        //For now run length is hardcoded
        BigQ bqL (*joinPtr->inputPipeL,*leftInterimPipe,oLeft,10);
        //For now run length is hardcoded
        BigQ bqR (*joinPtr->inputPipeR,*rightInterimPipe,oRight,10);
        
        //You may have to run duplicate removal on both the pipe
        
        /*Pass both these pipes to separate arrays and iterate over the
         arrays for join*/
        Record *newLeftRecord=new Record;
        vector<Record> *leftVector = new vector<Record>();
        while(leftInterimPipe->Remove(newLeftRecord)){
            
            leftVector->push_back(*newLeftRecord);
            //delete record
            delete newLeftRecord;
            newLeftRecord=NULL;
        
        }
        Record *newRightRecord=new Record;
        vector<Record> *rightVector = new vector<Record>();
        while(rightInterimPipe->Remove(newRightRecord)){
            
            rightVector->push_back(*newRightRecord);
            //delete record
            delete newRightRecord;
            newRightRecord=NULL;
            
        }
        
        
        int i=0, j= 0;
        while(i != leftVector->size() && j != rightVector->size()){
            
            /*Now perform join on both these vectors*/
            ComparisonEngine compEngine;
            
            if(compEngine.Compare(leftVector[0].data(), &oLeft, rightVector[0].data(), &oRight) == 0){
                
                if(compEngine.Compare(leftVector[0].data(), rightVector[0].data(), &joinPtr->recLiteral, &joinPtr->cnf) == 0){
                
                    /*Both the records match w.r.t. join attributes*/
                    
                    /*We need to merge them and put them into output pipe*/
                    int numAttrLeft = ((int *)leftVector[0].data()->bits)[1]/(sizeof (int))-1;
                    
                    int numAttrRight = ((int *)rightVector[0].data()->bits)[1]/(sizeof (int))-1;
                    
                    int attrToKeep[numAttrLeft + numAttrRight];
                    Record result;
                    result.MergeRecords(leftVector[0].data(), rightVector[0].data(), numAttrLeft, numAttrRight, attrToKeep, (numAttrLeft + numAttrRight), numAttrLeft);
                    
                    joinPtr->outputPipe->Insert(&result);
                    
                    
                    
                }
                
                
            }else if (compEngine.Compare(leftVector[0].data(), &oLeft, rightVector[0].data(), &oRight) > 1){
                /*Left record is greater*/
                j++;
                
                
            }else{
                /*Right record is greater*/
                i++;
                
            }
            
        }
        
        
    }
    //Do a nested block join
    else{
        
    }
    
    
}


