#include <stdio.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <cstdlib>
#include <unistd.h>

#include "data.h"
#include "parameters.h"

#ifdef Q2_SUBMIT_A
#include "../src/query2/dataStructQ2.h"
#include "parser/parser.h"
#else
#include "query2/dataStructQ2.h"
#include "lineparser/parser_includes.h"
#include "util/synchronization.h"
#endif


FILE *DataStructQ2::outputFile;

DataStructQ2::CellBucket *DataStructQ2::cellTable[DataStructQ2::GRID_SIZE][DataStructQ2::GRID_SIZE] = {NULL};
DataStructQ2::CellBucket DataStructQ2::initCellBuckets[DataStructQ2::CELL_BUCKETS_INIT];
DataStructQ2::TableSize_t DataStructQ2::cellBucketStackPtr;
DataStructQ2::TaxiBucket DataStructQ2::taxiTable[DataStructQ2::TAXI_HT_SIZE];
time_t DataStructQ2::currentTime;
clock_t DataStructQ2::currentDelay;
int DataStructQ2::errorFlag;
bool DataStructQ2::topsChanged;
DataStructQ2::FifoBucket *DataStructQ2::fifoLeftHead;
DataStructQ2::FifoBucket *DataStructQ2::fifo15minPtr;
DataStructQ2::FifoBucket *DataStructQ2::fifoRightTail;

DataStructQ2::MedianBucket *DataStructQ2::medianBucketStack[DataStructQ2::MEDIAN_STACK_SIZE];
int DataStructQ2::medianBucketStackPtr;
DataStructQ2::MedianBucket DataStructQ2::initMedianBuckets[DataStructQ2::MEDIAN_STACK_SIZE_INIT];
DataStructQ2::FifoBucket *DataStructQ2::fifoBucketStack[DataStructQ2::FIFO_STACK_SIZE];
int DataStructQ2::fifoBucketStackPtr;
DataStructQ2::FifoBucket DataStructQ2::initFifoBuckets[DataStructQ2::FIFO_STACK_SIZE_INIT];

DataStructQ2::CellBucket *DataStructQ2::topsTable[DataStructQ2::TOPS];
signed char DataStructQ2::topsSize;
DataStructQ2::CellBucket *DataStructQ2::mainHeap[DataStructQ2::CELL_TABLE_SIZE];
DataStructQ2::TableSize_t DataStructQ2::heapSize;

#ifdef Q2_DO_OUTPUT_THREAD
DataStructQ2::OutputFifoItem *DataStructQ2::ouputFifoRecyclerStack[DataStructQ2::OUTPUT_FIFO_RECYCLER_STACK_SIZE];
DataStructQ2::OutputFifoRecycler_t DataStructQ2::ouputFifoRecyclerStackPtr;
pthread_spinlock_t DataStructQ2::fifoRecyclerStackLock;
DataStructQ2::OutputFifoItem DataStructQ2::initOutputFifoItems[DataStructQ2::OUTPUT_FIFO_RECYCLER_STACK_SIZE];
pthread_mutex_t DataStructQ2::outputMutex;
pthread_cond_t DataStructQ2::outputCond;
bool DataStructQ2::cleanUpCalled;
bool DataStructQ2::outputThreadBlocked;
DataStructQ2::OutputFifoItem *DataStructQ2::outputFifoLeftHead;
DataStructQ2::OutputFifoItem *DataStructQ2::outputFifoRightTail;
#endif


#ifdef Q2_TIMING
clock_t DataStructQ2::Q2_acc;
long long int DataStructQ2::Q2_counter;
long long int DataStructQ2::Q2_counter_valid;
long long int DataStructQ2::Q2_counter_print;
#endif


void DataStructQ2::dataStructQ2InitFast(char *filename)
{
    //open file
    if (filename != NULL) outputFile = fopen(filename, "w");
    else outputFile = fopen("output2", "w");

    //clear flag and other variables
    errorFlag = 0;

    #ifdef Q2_DO_OUTPUT_THREAD
    pthread_spin_init(&fifoRecyclerStackLock, PTHREAD_PROCESS_PRIVATE);
    pthread_mutex_init(&outputMutex, NULL);
    pthread_cond_init (&outputCond, NULL);
    cleanUpCalled = false;
    outputThreadBlocked = true;
    outputFifoLeftHead = NULL;
    outputFifoRightTail = NULL;
    #endif
}


void DataStructQ2::dataStructQ2Init()
{
    currentTime = 0;
    topsChanged = false;

    //cell bucket stack pointer
    cellBucketStackPtr = CELL_BUCKETS_INIT - 1;

    //init sorting structure
    topsSize = 0;
    heapSize = 0;

    //init median recycler
    //push staticly allocated buckets into stack
    //this is cpu time that must be overlapped by other first io wait
    for (medianBucketStackPtr = 0; medianBucketStackPtr < MEDIAN_STACK_SIZE_INIT; medianBucketStackPtr++)
    {
        medianBucketStack[medianBucketStackPtr] = &(initMedianBuckets[medianBucketStackPtr]);
    }
    medianBucketStackPtr--; //points to the tops of the statck

    //init FIFO
    fifoLeftHead = NULL;
    fifo15minPtr = NULL;
    fifoRightTail = NULL;
    //push staticly allocated fifo buckets into stack
    //this is cpu time that must be overlapped by other first io wait
    for (fifoBucketStackPtr = 0; fifoBucketStackPtr < FIFO_STACK_SIZE_INIT; fifoBucketStackPtr++)
    {
        fifoBucketStack[fifoBucketStackPtr] = &(initFifoBuckets[fifoBucketStackPtr]);
    }
    fifoBucketStackPtr--; //points to the tops of the statck


    #ifdef Q2_DO_OUTPUT_THREAD
    //push staticly allocated fifo items into stack
    //this is cpu time that must be overlapped by other first io wait
    for (ouputFifoRecyclerStackPtr = 0;
            ouputFifoRecyclerStackPtr < OUTPUT_FIFO_RECYCLER_STACK_SIZE;
            ouputFifoRecyclerStackPtr++)
    {
        ouputFifoRecyclerStack[ouputFifoRecyclerStackPtr] = &(initOutputFifoItems[ouputFifoRecyclerStackPtr]);
    }
    ouputFifoRecyclerStackPtr--; //points to the tops of the stack
    #endif
}


void *DataStructQ2::query2_thread_main(void *)
{
    //init query 1 data struct
    dataStructQ2Init();

    //temp vars
    Record *rec;

    int counter = 0;

    while (true)
    {
        /*********************************/
        /*     Get record from parser    */
        /*********************************/
        #ifdef Q2_SUBMIT_A
        rec = Parser::getRecord(Q2_ID);
        if(rec == NULL) break;
        
        #else

        rec = RecordBuffer::getRecordQ2();
        counter++;

        if (!rec->valid)
        {
          if (rec->state & RECORD_EOF)
          {
            //printf("Q2 counter: %i\n", counter);
            break;
          }
          continue;
        }
        #endif
        /*********************************/
        /* End of get record from parser */
        /*********************************/


        #ifdef Q2_TIMING
        clock_t Q2_start = clock();
        #endif

        //update current time;
        currentTime = rec->dropoffTime;
        currentDelay = rec->startClockTick;

        /* pop all expired given the new current time */
        fifoPopBackAllExpired();

        /* check if taxi id is valid */
        if (rec->taxi.high == 0)
        {
            #ifdef Q2_TIMING
            Q2_acc += clock() - Q2_start;
            Q2_counter++;
            #endif
            continue;
        }

        /* update median */
        CellBucket *pickUpCellBucket = cellTable[rec->route600.cells.pickX][rec->route600.cells.pickY];
        //check if cell has never been used yet
        if (pickUpCellBucket == NULL)
        {
            //get a cell bucket
            popCellBucket(&pickUpCellBucket);
            //assign key to bucket
            cellTable[rec->route600.cells.pickX][rec->route600.cells.pickY] = pickUpCellBucket;
            pickUpCellBucket->x = rec->route600.cells.pickX + 1;
            pickUpCellBucket->y = rec->route600.cells.pickY + 1;
        }
        MedianBucket *tempMedianBucket; //for fifo
        if (rec->profit >= 0)
        {
            insertMedian(&(pickUpCellBucket->medianStruct), rec->profit, &tempMedianBucket);
            updateCellSorting(pickUpCellBucket);
        }
        else
        {
            pickUpCellBucket = NULL;
        }

        /* update empty taxis */
        CellBucket *dropoffCellBucket = cellTable[rec->route600.cells.dropX][rec->route600.cells.dropY];
        //check if cell has never been used yet
        if (dropoffCellBucket == NULL)
        {
            //get a cell bucket
            popCellBucket(&dropoffCellBucket);
            //assign key to bucket
            cellTable[rec->route600.cells.dropX][rec->route600.cells.dropY] = dropoffCellBucket;
            dropoffCellBucket->x = rec->route600.cells.dropX;
            dropoffCellBucket->y = rec->route600.cells.dropY;
        }
        dropoffCellBucket->emptyTaxis++;
        /* update place in heap or top ten */
        updateCellSorting(dropoffCellBucket);

        /* update taxis hash table */
        TaxiBucket *tempTaxiBucket;
        tempTaxiBucket = &( taxiTable[taxiHashFunc(rec->taxi.high)] );
        //find correct bucket and update
        while (1)
        {
            #ifdef Q2_SUBMIT_A
            if (tempTaxiBucket->key == rec->taxi.high &&
                tempTaxiBucket->key2 == rec->taxi.low)
            #else
            if (tempTaxiBucket->key == rec->taxi.high)
            #endif
            {
                //key found
                //if cellBucket ain't NULL decrease taxi
                //and unset fifo's taxiBucket so fifo can't use it again
                if (tempTaxiBucket->fifoBucket != NULL)
                {
                    tempTaxiBucket->cellBucket->emptyTaxis--;
                    updateCellSorting(tempTaxiBucket->cellBucket);
                    tempTaxiBucket->fifoBucket->taxiBucket = NULL;
                    //tempTaxiBucket->cellBucket = NULL; no need
                }
                break;
            }
            else if (tempTaxiBucket->nextBucket == NULL)
            {
                //key doesn't exist, create bucket if needed
                if ( tempTaxiBucket->key != TAXI_HT_GAURD_VALUE )
                {
                    tempTaxiBucket = (tempTaxiBucket->nextBucket = new TaxiBucket);
                }
                tempTaxiBucket->key = rec->taxi.high;
                #ifdef Q2_SUBMIT_A
                tempTaxiBucket->key2 = rec->taxi.low;
                #endif
                break;
            }
            else
            {
                tempTaxiBucket = tempTaxiBucket->nextBucket;
            }
        }
        //update bucket value
        tempTaxiBucket->cellBucket = dropoffCellBucket;

        /* push front in fifo */
        FifoBucket *fifoBucket;
        popFifoBucketRecycler(&fifoBucket);
        fifoBucket->inputTime = currentTime;
        fifoBucket->timestamp = rec->dropoffTimestamp;
        fifoBucket->taxiBucket = tempTaxiBucket;
        fifoBucket->medianCellBucket = pickUpCellBucket;
        fifoBucket->medianBucket = tempMedianBucket;
        //link to taxibucket
        tempTaxiBucket->fifoBucket = fifoBucket;
        fifoPushFront(fifoBucket);

        /* print output if needed */
        if (topsChanged)
        {
            //reset tops boolean
            topsChanged = false;

            #ifdef Q2_OUTPUT_STREAM
            #ifdef Q2_SUBMIT_A
            //record has all information needed
            printOutput( rec->dropoffTimestamp, 0, 0, rec);
            #else
            //offset is negative for pickup time
            printOutput(rec->dropoffTimestamp, currentTime, -(rec->duration));
            #endif
            #endif
        }

        #ifdef Q2_TIMING
        Q2_acc += clock() - Q2_start;
        Q2_counter++;
        Q2_counter_valid++;
        #endif
    }

    cleanUp();
    pthread_exit(NULL);
}


void DataStructQ2::cleanUp()
{
    //add an hour to current time
    //currentTime += 3600;
    //pop all expired from fifo
    //fifoPopBackAllExpired();

    #ifdef Q2_DO_OUTPUT_THREAD
    pthread_mutex_lock(&outputMutex);
    //cout<<"cleanup set to true"<<endl;
    cleanUpCalled = true;
    pthread_cond_signal(&outputCond);
    pthread_mutex_unlock(&outputMutex);
    #else
    //close output file
    fclose(outputFile);  
    #endif

    //printf("Q2 main thread exiting...\n");
}


inline void DataStructQ2::popCellBucket(CellBucket** returnPtr)
{
    //static int callocs=0;
    //if stack is NOT empty
    if (cellBucketStackPtr >= 0)
    {
        *returnPtr = &(initCellBuckets[cellBucketStackPtr]);
        cellBucketStackPtr--;
    }
    //stack is empty
    else
    {
        //calloc also sets to 0
        *returnPtr = (CellBucket*)calloc(1, sizeof(CellBucket));
        //cout << "cell MALLOCED" <<endl;
        //cout<<++callocs<<endl;
    }
}

inline DataStructQ2::TableSize_t DataStructQ2::taxiHashFunc(unsigned long long int high /*, long long low */)
{
    return (high % 2147483647 ) % TAXI_HT_SIZE;
    //return (static_cast<TableSize_t>(high>>32) | static_cast<TableSize_t>(high)
    //         | static_cast<TableSize_t>(low>>32) | static_cast<TableSize_t>(low));
    //return ( (1485500463*
    //        (static_cast<TableSize_t>(high>>32) | static_cast<TableSize_t>(high)
    //         | static_cast<TableSize_t>(low>>32) | static_cast<TableSize_t>(low))
    //        + 1179660437) % 2147483647 ) % TAXI_HT_SIZE;
}



/** FIFO FUNCTIONS **/
inline void DataStructQ2::popFifoBucketRecycler(FifoBucket **returnPtr)
{
    //static int mallocs=0;
    //case of NOT empty stack
    if (fifoBucketStackPtr >= 0)
    {
        *returnPtr = fifoBucketStack[fifoBucketStackPtr];
        fifoBucketStackPtr--;
    }
    //case of empty stack
    else
    {
        *returnPtr = (FifoBucket *)malloc(sizeof(FifoBucket));
        //cout << "fifo MALLOCED" <<endl;
        //cout<<++mallocs<<endl;
    }
}

inline void DataStructQ2::fifoPushFront(FifoBucket *bucket)
{
    //link to FIFO
    if (fifoLeftHead == NULL)
    {
        //empty FIFO
        fifoLeftHead = fifoRightTail = fifo15minPtr = bucket;
        bucket->left = NULL;
    }
    else
    {
        fifoLeftHead->left = bucket;
        bucket->left = NULL;
        fifoLeftHead = bucket;
    }

}

inline void DataStructQ2::fifoPopBackAllExpired()
{
    //check for empty list
    if (fifoRightTail == NULL) {return;}

    //check if all 15min bucket are dealt with
    if (fifo15minPtr != NULL)
    {
        //pop all expired for 15 minutes window
        while (fifo15minPtr->inputTime + 900 <= currentTime)
        {
            if (fifo15minPtr->medianCellBucket != NULL)
            {
                deleteMedian(&(fifo15minPtr->medianCellBucket->medianStruct), fifo15minPtr->medianBucket);
                updateCellSorting(fifo15minPtr->medianCellBucket);

                /* print output if needed */
                if (topsChanged)
                {
                    //reset tops boolean
                    topsChanged = false;

                    #ifdef Q2_OUTPUT_STREAM

                    #ifdef Q2_SUBMIT_A
                    printOutput(fifo15minPtr->timestamp, fifo15minPtr->inputTime, 900, NULL);
                    #else
                    printOutput(fifo15minPtr->timestamp, fifo15minPtr->inputTime, 900);
                    #endif
                    
                    #endif
                }
            }

            //move towards head
            fifo15minPtr = fifo15minPtr->left;

            if (fifo15minPtr == NULL) break;
        }
    }


    //pop all expired for 30min window
    while (fifoRightTail->inputTime + 1800 <= currentTime)
    {
        //remove taxi if it hasn't already been removed
        if (fifoRightTail->taxiBucket != NULL)
        {
            //decrease emptyTaxis and update cellsorting
            fifoRightTail->taxiBucket->cellBucket->emptyTaxis--;
            updateCellSorting(fifoRightTail->taxiBucket->cellBucket);

            /* print output if needed */
            if (topsChanged)
            {
                //reset tops boolean
                topsChanged = false;

                #ifdef Q2_OUTPUT_STREAM

                #ifdef Q2_SUBMIT_A
                printOutput(fifoRightTail->timestamp, fifoRightTail->inputTime, 1800, NULL);
                #else
                printOutput(fifoRightTail->timestamp, fifoRightTail->inputTime, 1800);
                #endif
                    
                #endif
            }

            //set fifoBucket to NULL so it won't be removed twice
            fifoRightTail->taxiBucket->fifoBucket = NULL;
            //fifoRightTail->taxiBucket->cellBucket = NULL;
        }

        //if fifo has only 1 item left
        if (fifoRightTail->left == NULL)
        {
            //now fifo will be empty
            pushFifoBucketRecycler(fifoRightTail);
            fifoRightTail = fifoLeftHead = fifo15minPtr = NULL;
            break;
        }
        else
        {
            FifoBucket *temp = fifoRightTail;
            fifoRightTail = fifoRightTail->left;
            pushFifoBucketRecycler(temp);
        }
    }
}


inline void DataStructQ2::pushFifoBucketRecycler(FifoBucket *bucket)
{
    //case of NOT full stack
    if (fifoBucketStackPtr < FIFO_STACK_SIZE - 1)
    {
        fifoBucketStackPtr++;
        fifoBucketStack[fifoBucketStackPtr] = bucket;
    }
    //case of full stack and bucket is not part of initFifoBuckets
    //so free can be called
    else if ( bucket < initFifoBuckets || bucket >= initFifoBuckets + FIFO_STACK_SIZE_INIT)
    {
        //cout << "fifo FREED" <<endl;
        free(bucket);
    }
}
/** END OF FIFO FUNCTIONS **/



/****************************************************************/
/********************* DEBUG AND TIMING CODE ********************/
/****************************************************************/
#ifdef Q2_COMPILE_DEBUG_CODE
void DataStructQ2::printOuputMessages()
{
    #ifdef Q2_TIMING
    printf("\nQ2 avg time in sec: %lf\n", ((double)Q2_acc / Q2_counter) / CLOCKS_PER_SEC);
    printf("Q2 avg time in tics: %lf\n", (double)Q2_acc / Q2_counter);
    printf("Q2 whole time in tics %ld\n", Q2_acc);
    printf("Q2 whole time in secs %lf\n", (double)Q2_acc / CLOCKS_PER_SEC);
    printf("Q2 got %lld records of which %lld where valid.\n", Q2_counter, Q2_counter_valid);
    printf("Q2 printed %lld times.\n\n\n", Q2_counter_print);
    #endif
}
#endif
/****************************************************************/
/***************** END OF DEBUG AND TIMING CODE *****************/
/****************************************************************/
