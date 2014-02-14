#ifndef DBFILE_H
#define DBFILE_H



#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

#include <iostream>

typedef enum {heap, sorted, tree} fType;


class BaseDBFile{
    
public:
    BaseDBFile ();
    ~BaseDBFile();
    
    
	virtual int Create (char *fpath, fType file_type, void *startup)=0;
	virtual int Open (char *fpath)=0;
	virtual int Close ()=0;
    
	virtual void Load (Schema &myschema, char *loadpath);
    
	virtual void MoveFirst ()=0;
	virtual void Add (Record &addme)=0;
	virtual int GetNext (Record &fetchme)=0;
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal)=0;
};


// stub DBFile header..replace it with your own DBFile.h

class DBFile  {

private:
    
    File file;
    Page page;
    bool isFileOpen;
    int currentPageNumber;
    int lastPageNumber;
    char* fileName;
    bool isPageDirty;
    
    
public:
	DBFile ();
    virtual ~DBFile();
    
    int Create (char *fpath, fType file_type, void *startup);
    
	int Open (char *fpath);
	int Close ();
    
	void Load (Schema &myschema, char *loadpath);
    
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    void WritePageToFileIfDirty(Page* page, int);
    void initializePage();
    
    
};
#endif



