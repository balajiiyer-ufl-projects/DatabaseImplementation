#ifndef SORTED_H
#define SORTED_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include <iostream>


class Sorted : public DBFile {

private:
    File file;
    Page page;
    string metadataFile;
    bool isFileOpen;
    int currentPageNumber;
    int lastPageNumber;
    char* fileName;
    bool isPageDirty;
    void WriteMetadata();
    
    
public:
	Sorted ();
    ~Sorted();
    
	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();
    
	void Load (Schema &myschema, char *loadpath);
    
	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
    void WritePageToFileIfDirty(Page* page, int);
    //Returns the metadata file name
    inline string GetMetadataFile(){
        return metadataFile;
    }
    
};
#endif
