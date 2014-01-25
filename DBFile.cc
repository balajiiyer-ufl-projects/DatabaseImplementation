#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

#include <iostream>

// stub file .. replace it with your own DBFile.cc
using namespace std;

DBFile *dbfile;


DBFile::DBFile () {
    
}
/*This function will create a file on the disk. This function
should be called only once when you are loading the records.
*/
int DBFile::Create (char *f_path, fType f_type, void *startup) {
    
    this->file.Open(0, f_path);
    
    /*This checks if the file opened successfully or not*/    
    if(this->file.GetFileStatus()<0)
        return 0;

    cout<<"File opened successfully during creation"<<endl;    

    /*Once file is opened successfully, create a page instance*/
    initializePage();

    return 1;
}

/*This function loads the records from a text file to the actual database file on the disk*/
void DBFile::Load (Schema &f_schema, char *loadpath) {
    
    FILE *fileToLoad = fopen(loadpath, "r");
   
    if(!fileToLoad){
        cout<< "Can't open file name :" + string(loadpath);
    }

    Record record;
 
    /*Get all the records one by one from the 
      text file and using schema file, convert them to database record format.
      And now append those records to the page*/
    while(record.SuckNextRecord(&f_schema,fileToLoad)){
        Add(record);
    }
    //FIXME check the page number and also reference or pointer
    /*Once all the records are added, write the page to the disk*/
//    WritePageToFileIfDirty(&this->page, this->currentPageNumber);

    cout<<"Records loaded successfully"<<endl;
}

/*This function opens a file when path to the file is provided
as a parameter
*/
int DBFile::Open (char *f_path) {
    /*Since file is already created when open function is called,
	it's length is non-zero. Since we do not know the exact length 
	we are sending some number i.e.1 */
    file.Open(1, f_path);

    /*This checks if the file was opened successfully*/
    if(this->file.GetFileStatus()<0)
        return 0;

    cout<<"File opened successfully"<<endl;
	
    /*Once file is opened successfully, create a page instance*/
    initializePage();

    return 1;
    
    
}

void DBFile::MoveFirst () {
    
    cout<<"Moving to first page"<<endl;
    /*Since we are moving to the first page, page number is 1*/
    this->file.GetPage(&this->page, 1);
    this->currentPageNumber = 1;
    
}
/*This function closes the file.*/
int DBFile::Close () {

   /*Before closing the file, it is necessary to write the page to the disk.*/ 
    WritePageToFileIfDirty(&this->page, this->currentPageNumber);
    return this->file.Close();
}

void DBFile::initializePage(){

	/*Since page is created just now, it does not represent any page of a file in the memory.
	Hence currentPageNumber is 0.
	Also currently nothing is written on the page, hence it is not dirty*/
	//Page p;
	//this->page = p;
	//FIXME
	this->currentPageNumber = 0;
	this->isPageDirty = false;

	cout<<"Buffer instance is initialized successfully."<<endl;

}
/*This function is used to add records to the page and in turn to the database file*/
void DBFile::Add (Record &rec) {

	/*This function should check if the current page number is 0.
	If it is then it should set it to 1*/
	
	if(this->currentPageNumber == 0){
		this->currentPageNumber++;
		cout<< "Setting current page to " << this->currentPageNumber << " during add " << endl;
	}
	
	cout << "No os records in page are "<< this->page.GetNumRecs() << endl;
	
	/*This function will take care of writing the page out 
	if the page becomes full before adding a record.

	But it will NOT take care of writing the page out once all records are added and
	still page is not full. Calling function should take care of it*/

	/*Check if the page is full before adding a record*/
	if(!this->page.Append(&rec)){
	
		//FIXME
		WritePageToFileIfDirty(&this->page, this->currentPageNumber);
		cout << "Page is full.Appending the record to next page."<<endl;
		this->currentPageNumber++;
	
		/*Here, File::GetPage function can't be called. This is because 
		that function returns an existing page whereas here we are adding a new page.
		But WritePageToFile... takes care of cleaning up of page instance, hence we 
		just have to append the records to the new page*/
		
		/*Now load further records*/
		this->page.Append(&rec);
		/*Since we added a record, we should set page as dirty*/
		this->isPageDirty = true;
						
	}else{
		cout << "Records appended" << endl;
		/*This means record is appended so set page as dirty*/
		this->isPageDirty = true;
	}
			
    
}


/*This function writes a page to the disk ONLY if it is dirty*/
void DBFile :: WritePageToFileIfDirty(Page* page, int whichPage){
	    
    /*Write dirty page to the file*/
    if(this->isPageDirty){
    	
        this->file.AddPage(page, whichPage);
        
        this->page.EmptyItOut();
	
	/*NOTE:: We need not increment the current page number in this function.
	The reason is we are not sure what will be the next operation after writing the page.
	Hence we should deligate that responsiblity to the next function.
	*/

    }

    
}

int DBFile::GetNext (Record &fetchme) {
    
    cout << "Getting next record"<<endl;

      /*For each page, perform getPage to bring the page into the buffer.
      Then perform GetFirst() to get records from the page. 
      The moment GetFirst returns 0 means scanning of that page is completed.
      So fetch next page i.e. continue the loop.
    */

    	int noOfPagesInFile = (this->file.GetLength()) - 1; 
	/*Decrementing the length by 1 because first page of the file
        does not contain the data*/
        
	cout<< "File length is :: " << noOfPagesInFile <<endl;
	if(this->page.GetFirst (&fetchme)){
		cout << "Should print records" <<endl;
		return 1;
	}else{
		
		/*This means that we have scanned all the records on that page
		So we should move to next page
		*/
		this->page.EmptyItOut();
		this->currentPageNumber++;

//		cout << "Current Page number is " <<  this->currentPageNumber << endl;	
//		cout << "No of pages in file is " <<  noOfPagesInFile << endl;

		if(this->currentPageNumber < noOfPagesInFile){
			this->file.GetPage(&this->page, this->currentPageNumber);
			if(this->page.GetFirst(&fetchme)){
				return 1;
			}
			else{
				//FIXME this case should never occur ideally
				return 0;
			}
		}else{
			/*This means that we have scanned the file till the end*/
			return 0;
		}

	}
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
	ComparisonEngine comp;
    
	while(GetNext(fetchme)){
        	if(comp.Compare(&fetchme, &literal, &cnf)) {
			cout << "here here here";
	        	return 1;
        	}
    	}	
    return 0;
}
