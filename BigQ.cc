#include "BigQ.h"

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
 
    // read data from in pipe sort them into runlen pages

    //init data structures
    inPipe = &in;
    outPipe = &out;
    sortOrder = &sortorder;
    fileName="runFile"; //probably requires timeStamp;


    
    
    
    
    
    
    // construct priority queue over sorted runs and dump sorted data
 	// into the out pipe

    // finally shut down the out pipe
	out.ShutDown ();
}

BigQ::~BigQ () {
}
