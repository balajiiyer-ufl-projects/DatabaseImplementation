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
    pthread_join (thread, 0);
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
    Record *newRecord=new Record;
    Record *oldRecord=new Record();
    ComparisonEngine compEng;
    bool firstIteration=true;
    while(interimPipe->Remove(newRecord)){
        if(firstIteration){
            oldRecord->Copy(newRecord);
            firstIteration=false;
        }
        //If the records are not equal,add the new record
        //to output pipe and set the old record to new record
        if(compEng.Compare(oldRecord,newRecord, &myOrder)!=0){
            dr->outputPipe->Insert(oldRecord);
            oldRecord->Copy(newRecord);
        }
    }
    dr->outputPipe->Insert(oldRecord);
    
    interimPipe->ShutDown();
    dr->outputPipe->ShutDown();
    delete oldRecord;
    delete interimPipe;
}

void DuplicateRemoval::WaitUntilDone () {
    pthread_join (thread, 0);
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
    
    double sum = 0;
    
    while(sumPtr->inputPipe->Remove(&fetchme)){
        int ival = 0;
        double dval = 0;
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
    
    pthread_join (thread, 0);
    
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
            //delete newRecord;
            //newRecord=NULL;
            
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
            firstIteration=true;
            //oldRecord->Copy(newRecord);
            
            //CULPRIT :Need to calculate the sum of last record if any
            if(newRecord!=NULL)
            {
                int ival = 0; double dval = 0;
                grpBy->funcPtr->Apply(*newRecord, ival, dval);
                sum = (ival + dval);
                //delete newRecord;
                //newRecord= NULL;
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
    
    pthread_join (thread, 0);
    
}
void GroupBy::Use_n_Pages (int n) { }



void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    inputPipeL=&inPipeL;
    inputPipeR=&inPipeR;
    outputPipe=&outPipe;
    cnf=&selOp;
    recLiteral=&literal;
    pthread_create(&thread,NULL,&JoinRecords,(void*)this);
    return;
    
    
}
void Join::WaitUntilDone(){
    pthread_join (thread, NULL);
}

void Join::Use_n_Pages (int runlen) {
    
}

void* Join::JoinRecords(void *ptr){
    
    Join* joinPtr=(Join*)ptr;
    OrderMaker oLeft,oRight;
    
    joinPtr->cnf->GetSortOrders(oLeft, oRight);
    
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
        /*1.Fill the run vectors until the record differs
         2.Once the left and right have been filled,then merge them
         3.
         
         
         */
        Record rightRecord;
        Record leftRecord;
        Record *oldRecord=NULL;
        Record *newRecord=NULL;
        vector<Record *> leftVector;
        vector<Record *> rightVector;
        bool doleft=true;
        bool doright=true;
        ComparisonEngine compEngine;
        
        
        bool leftPendingRecord=false;
        bool rightPendingRecord=false;
        
        while(doleft || doright){
            
#ifdef RELOP
            cout<<"Fetching from left pipe"<<endl;
#endif
            //Fetch records from left pipe
            if(doleft){
                oldRecord=newRecord=NULL;
                if(leftPendingRecord){
                    
                    Record *copyRec=new Record;
                    copyRec->Copy(&leftRecord);
                    leftVector.push_back(copyRec);
                    newRecord=copyRec;
                    leftPendingRecord=false;
                    
                }
                while(leftInterimPipe->Remove(&leftRecord)){
                    #ifdef RELOP
                    cout << "Inside left vector loop" << endl;
                    #endif
                    
                    Record *copyRec=new Record;
                    copyRec->Copy(&leftRecord);
                    oldRecord=newRecord;
                    newRecord=copyRec;
#ifdef RELOP
                    cout<<"Comparing records on left"<<endl;
#endif
                    if((oldRecord==NULL) || (oldRecord&&compEngine.Compare(oldRecord,newRecord, &oLeft)==0)){
                        leftVector.push_back(copyRec);
                    }
                    else{
                        
                        leftPendingRecord=true;
                        delete copyRec;
                        copyRec=NULL;
                        break;
                    }
                    
                }
                if(!leftPendingRecord)
                {
                    doleft = false;
                    leftInterimPipe->ShutDown();
                }
            }
#ifdef RELOP
            cout<<"Fetch records from right pipe"<<endl;
#endif
            if(doright){
                
                oldRecord=newRecord=NULL;
                if(rightPendingRecord){
                    Record *copyRec=new Record;
                    copyRec->Copy(&rightRecord);
                    rightVector.push_back(copyRec);
                    newRecord=copyRec;
                    rightPendingRecord=false;
                    
                }
                
                while(rightInterimPipe->Remove(&rightRecord)){
#ifdef RELOP
                    cout << "Inside right vector loop" << endl;
#endif
                    
                    Record *copyRec=new Record;
                    copyRec->Copy(&rightRecord);
                    oldRecord=newRecord;
                    newRecord=copyRec;
#ifdef RELOP
                    cout<<"Comparing records on right"<<endl;
#endif
                    if((oldRecord==NULL)||(oldRecord&&compEngine.Compare(oldRecord,newRecord, &oRight)==0)){
                        rightVector.push_back(copyRec);
                    }
                    else{
                        rightPendingRecord=true;
                        delete copyRec;
                        copyRec=NULL;
                        break;
                    }
                    
                }
                if(!rightPendingRecord)
                {
                    doright = false;
                    rightInterimPipe->ShutDown();
                }
            }
#ifdef RELOP
            cout<<"Joining records here"<<endl;
            cout<<"Vector Size records here : "<<leftVector.size()<<" : "<<rightVector.size()<<endl;
            cout<<"DoLeft : Do Right " <<doleft<< " : "<<doright;
#endif
            if(leftVector.size()>0&&rightVector.size()>0){
                
#ifdef RELOP
                cout<<"Calculating the attribute length"<<endl;
#endif
                /*Calculate the attribute length*/
                int numAttrLeft = ((int *)leftVector.at(0)->bits)[1]/(sizeof (int))-1;
                int numAttrRight = ((int *)rightVector.at(0)->bits)[1]/(sizeof (int))-1;
                int attrToKeep[numAttrLeft + numAttrRight];
                //intialize attrToKeep
                int i,j=0;
                for(i=0;i<numAttrLeft;i++){
                    attrToKeep[i]=i;
                }
                for(j=0;j<numAttrRight;j++){
                    attrToKeep[i++]=j;
                }
                Record result;
                ComparisonEngine coe;
#ifdef RELOP
                cout<<"Comparing records for merging"<<endl;
#endif
                //If the records are equal,merge and send it to output pipe
                if(coe.Compare(leftVector.at(0), &oLeft, rightVector.at(0), &oRight) == 0){
                    if(coe.Compare(leftVector.at(0), rightVector.at(0), joinPtr->recLiteral, joinPtr->cnf) == 1)
                    {
                        for(int leftCount=0;leftCount<leftVector.size();leftCount++){
                            for(int rightCount=0;rightCount<rightVector.size();rightCount++){
                                //cout << "Record schema is compared" << endl;
                                //if(coe.Compare(leftVector.at(leftCount), rightVector.at(rightCount), joinPtr->recLiteral, joinPtr->cnf) == 1){
                                /*Both the records match w.r.t. join attributes*/
#ifdef RELOP
                                cout << "Both the records match" << endl;
#endif
                                /*We need to merge them and put them into output pipe*/
                                //for(int leftCount=0;leftCount<leftVector.size();leftCount++){
                                //  for(int rightCount=0;rightCount<rightVector.size();rightCount++){
                                //Record result;
                                Record copyRec;
                                copyRec.Copy(rightVector.at(rightCount));
                                result.MergeRecords(leftVector.at(leftCount),&copyRec, numAttrLeft, numAttrRight, attrToKeep, (numAttrLeft + numAttrRight), numAttrLeft);
                                joinPtr->outputPipe->Insert(&result);
                            }
                        }
                        //                        while(!leftVector.empty()) delete leftVector.back(), leftVector.pop_back();
                        //
                        //                        while(!rightVector.empty()) delete rightVector.back(), rightVector.pop_back();
                        //
                        //                        leftVector.clear();
                        //                        rightVector.clear();
                        
                    }
                    
                    Clear(leftVector);
                    Clear(rightVector);
                    doleft=true;
                    doright=true;
                }
                //Left is lesser than right
                else if (compEngine.Compare(leftVector.at(0), &oLeft, rightVector.at(0), &oRight) <0 ){
#ifdef RELOP
                    cout<<"Left is lesser than right"<<endl;
#endif
                    doleft=true;
                    doright=false;
                    Clear(leftVector);
                    
                }
                //Right is lesser than left
                //else if(compEngine.Compare(leftVector.at(0), &oLeft, rightVector.at(0), &oRight) > ){
                else{
#ifdef RELOP
                    cout<<"right is lesser than left"<<endl;
#endif
                    doleft=false;
                    doright=true;
                    Clear(rightVector);
                }
                
            }
            
            else{
#ifdef RELOP
                cout<<"I am here in else";
#endif
                break;
            }
            
        }
        
        //            else{
        //                break;
        //            }
        //            //If no records in left and right pipe,exit.
        //            if(!recordsInLeftPipe&&!recordsInRightPipe){
        //                break;
        //            }
        
    }
    
    //}
    //nested block join
    else{
#ifdef RELOP
        cout<<"I am here"<<endl;
#endif
        
    }
    joinPtr->outputPipe->ShutDown();
}

void Join::Clear(vector<Record*> &value)
{
    while(!value.empty()) delete value.back(), value.pop_back();
    value.clear();
    
    //    Record *rec;
    //    for (int i = 0; i < v.size(); i++)
    //    {
    //        rec = v.at(i);
    //        delete rec;
    //        rec = NULL;
    //    }
    //    v.clear();
}
void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    schema = &mySchema;
    inputPipe = &inPipe;
    outf = outFile;
    
    pthread_create(&thread,NULL,&WriteOutToFile,(void*)this);
    return;
}

void* WriteOut::WriteOutToFile(void *ptr) {
    WriteOut *writeptr = (WriteOut*)ptr;
    
    //cout<<"Num of Att: "<<numAtts<<endl;
    Record *newRec=new Record;
    //	cout<<"Begin WriteOut"<<endl;
    while(writeptr->inputPipe->Remove(newRec)) {
        int numAtts = writeptr->schema->GetNumAtts();
        Attribute *atts = writeptr->schema->GetAtts();
        
        for(int i=0;i<numAtts;i++) {
            //          cout<<"Att Name"<<atts[i].name<<endl;
            
            fprintf(writeptr->outf,"%s",atts[i].name);
            int offset = ((int *)newRec->bits)[i+1];
            fprintf(writeptr->outf,"[");
            if(atts[i].myType==Int) {
                // cout<<"Type is :"<<atts[i].myType<<endl;
                int *inte = (int *)&(newRec->bits[offset]);
                fprintf(writeptr->outf,"%d",*inte);
            }
            else if(atts[i].myType==Double) {
                double *doub = (double *)&(newRec->bits[offset]);
                fprintf(writeptr->outf,"%f",*doub);
            }
            else if(atts[i].myType==String) {
                char *stringatt = (char *)&(newRec->bits[offset]);
                fprintf(writeptr->outf,"%s",stringatt);
            }
            fprintf(writeptr->outf,"]");
            if(i!=numAtts-1){
                fprintf(writeptr->outf,",");
            }
        }
        
        fprintf(writeptr->outf,"\n");
    }
    //cout<<"End WriteOut";
    fclose(writeptr->outf);
}

void WriteOut::WaitUntilDone() {
    pthread_join(thread,0);
}

void WriteOut::Use_n_Pages(int n) {
    
}

