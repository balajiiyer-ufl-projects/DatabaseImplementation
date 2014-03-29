#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class RelationalOp {
public:
	// blocks the caller until the particular relational operator
	// has run to completion
	virtual void WaitUntilDone () = 0;
    
	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp {
    
private:
	pthread_t thread;
	// Record *buffer;
    
    DBFile *inputFile;
    Pipe *outputPipe;
    CNF cnf;
    Record recLiteral;
    
public:
    
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
    static void *GetDataFromFileToPipe(void *ptr);
    
};

class SelectPipe : public RelationalOp {
    
private:
	pthread_t thread;
    Pipe *inputPipe;
    Pipe *outputPipe;
    CNF cnf;
    Record recLiteral;
	
public:
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
    static void *GetDataFromInputToOutputPipe(void *ptr);
};
class Project : public RelationalOp {
private:
    pthread_t thread;
    Pipe *inputPipe;
    Pipe *outputPipe;
    int  *attToKeep;
    int attNumInput;
    int attNumOutput;
    
public:
	void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
    static void *ProjectFromInputToOutputPipe(void *ptr);
};
class Join : public RelationalOp {
private:
    pthread_t thread;
    Pipe *inputPipeL;
    Pipe *inputPipeR;
    Pipe *outputPipe;
    CNF cnf;
    Record recLiteral;
    
    
public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
    static void* JoinRecords(void *ptr);
};
class DuplicateRemoval : public RelationalOp {
private:
    pthread_t thread;
    Pipe *inputPipe;
    Pipe *outputPipe;
    Schema* schema;
    static int runLength;
    
public:
    
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
    static void *RemoveDuplicates(void *ptr);
};
class Sum : public RelationalOp {
private:
	pthread_t thread;
	Pipe *inputPipe;
	Pipe *outputPipe;
	Function *funcPtr;
public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	static	void* GetSum(void *ptr);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class GroupBy : public RelationalOp {
private:
	pthread_t thread;
	Pipe *inputPipe;
	Pipe *outputPipe;
    OrderMaker myOrder;
	Function *funcPtr;
    
    
public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
    static void* GroupAndSum(void *ptr);
};
class WriteOut : public RelationalOp {
public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};
#endif
