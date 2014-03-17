#include "Pipe.h"

#include <iostream>
#include <stdlib.h> 


Pipe :: Pipe (int bufferSize) {

	// set up the mutex assoicated with the pipe
	pthread_mutex_init (&pipeMutex, NULL);

	// set up the condition variables associated with the pipe
	pthread_cond_init (&producerVar, NULL);
	pthread_cond_init (&consumerVar, NULL);

	// set up the pipe's buffer
	buffered = new (std::nothrow) Record[bufferSize];
	if (buffered == NULL)
	{
		cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
	}

	totSpace = bufferSize;
	firstSlot = lastSlot = 0;

	// note that the pipe has not yet been turned off
	done = 0;
}

Pipe :: ~Pipe () {

	// free everything up!
	delete [] buffered;

	pthread_mutex_destroy (&pipeMutex);
	pthread_cond_destroy (&producerVar);
	pthread_cond_destroy (&consumerVar);
	
}


void Pipe :: Insert (Record *insertMe) {
#ifdef DEBUG
	//cout<<"Pipe : Inserting record"<<endl;
#endif
    if (!done){

	// first, get a mutex on the pipeline
	pthread_mutex_lock (&pipeMutex);

	// next, see if there is space in the pipe for more data; if
	// there is, then do the insertion
	if (lastSlot - firstSlot < totSpace) {
		buffered [lastSlot % totSpace].Consume (insertMe);
        
	//cout<<"Pipe: Insertion done"<<endl;
	// if there is not, then we need to wait until the consumer
	// frees up some space in the pipeline
	} else {
		pthread_cond_wait (&producerVar, &pipeMutex);
		buffered [lastSlot % totSpace].Consume (insertMe);
	}
	
	// note that we have added a new record
	lastSlot++;
	//cout<<"Pipe : No of records inserted="<<lastSlot<<endl;
	// signal the consumer who might now want to suck up the new
	// record that has been added to the pipeline
	pthread_cond_signal (&consumerVar);

	// done!
	pthread_mutex_unlock (&pipeMutex);
    }
}


int Pipe :: Remove (Record *removeMe) {
	 
	// first, get a mutex on the pipeline
	pthread_mutex_lock (&pipeMutex);

	// next, see if there is anything in the pipeline; if
	// there is, then do the removal
	if (lastSlot != firstSlot) {
		//cout<<"Pipe :Record is present in the pipe"<<endl;
		removeMe->Consume (&buffered [firstSlot % totSpace]);

	// if there is not, then we need to wait until the producer
	// puts some data into the pipeline
	} else {

		// the pipeline is empty so we first see if this
		// is because it was turned off
		if (done) {

			pthread_mutex_unlock (&pipeMutex);
			return 0;
		}

		// wait until there is something there
		//cout<<"Waiting in pthread remove"<<endl;
		pthread_cond_wait (&consumerVar, &pipeMutex);
		//cout<<"Not reachable"<<endl;
		// since the producer may have decided to turn off
		// the pipe, we need to check if it is still open
		if (done && lastSlot == firstSlot) {
			pthread_mutex_unlock (&pipeMutex);
			return 0;
		}

		removeMe->Consume (&buffered [firstSlot % totSpace]);
	}
	
	// note that we have deleted a record
	firstSlot++;
	//cout<<"Deleted a record.First slot advanced"<<endl;
	// signal the producer who might now want to take the slot
	// that has been freed up by the deletion
	pthread_cond_signal (&producerVar);
	
	// done!
	pthread_mutex_unlock (&pipeMutex);
	//cout<<"Pipe : returning 1"<<endl;
	return 1;
}

void Pipe :: Open (){
    done = 0;
}

void Pipe :: ShutDown () {

	// first, get a mutex on the pipeline
        pthread_mutex_lock (&pipeMutex);

	// note that we are now done with the pipeline
	done = 1;

	// signal the consumer who may be waiting
	pthread_cond_signal (&consumerVar);

	// unlock the mutex
	pthread_mutex_unlock (&pipeMutex);
	
}
