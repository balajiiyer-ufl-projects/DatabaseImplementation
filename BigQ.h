#ifndef BIGQ_H
#define BIGQ_H

#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Comparison.h"
#include <vector>
//#include <algorithm>
#include <queue>

using namespace std;


class BigQ
{
//Phase - 1
private:
    char* fileName;
    File file;
    vector<int>runIndices;
    int pagesTotal;
    Pipe *inPipe, *outPipe;
    OrderMaker *sortOrder;
    int runLength;
    ComparisonEngine ce;
    int appendCount;
    int pageCount;
  
public:
    BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlength);
    ~BigQ();
    //Phase-1
   static void *SortPhase1(void *ptr);
    void* CreateSortedRuns();
    //void writePages( vector<Record *> &pqRecs);
};

class CompareRec
{
private:
    OrderMaker* sortOrder;
public:
    CompareRec(OrderMaker *sortorder)
    {
        sortOrder = sortorder;
    }
    bool operator()(Record* left,Record* right)
    {
        ComparisonEngine comparisonEngine;
        if(comparisonEngine.Compare(left,right,sortOrder)<0)
            return false;
        else
            return true;
    }
};


#endif