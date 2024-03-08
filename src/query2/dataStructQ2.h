#ifndef DATASTRUCTQ2_H_
#define DATASTRUCTQ2_H_

#include "data.h"
#include "parameters.h"

#include <time.h>
#include <pthread.h>

class DataStructQ2
{
public:
    //typedef long long int RouteHtKey_t;
    //typedef long long int TaxiHtKey_t [2]; //[0] is high, [1] is low
    typedef int TableSize_t;
    typedef short int CellIndex_t;
    typedef unsigned long long int TaxiHtKey_t;
    typedef int ProfitKey;
    typedef short int EmptyTaxisKey;
    static const TableSize_t CELL_TABLE_SIZE = 360000;
    static const TableSize_t CELL_BUCKETS_INIT = 100000;
    static const TableSize_t GRID_SIZE = 600;
    static const TableSize_t TAXI_HT_SIZE = 150000;
    static const signed char TOPS = 10;
    static const signed char TAXI_HT_GAURD_VALUE = 0;
    //median
    typedef short int HeapSize_t;
    typedef int MedianKey;
    //static const MedianKey MEDIAN_KEY_GAURD_VAULE = -1;
    static const HeapSize_t MAX_HEAP = 255; //63;
    //static const HeapSize_t FIRST_LEAF = 31;
    //recyclers
    static const int FIFO_STACK_SIZE = 50000; //50000; //80000;
    static const int FIFO_STACK_SIZE_INIT = 30000; //50000; //80000;
    static const int MEDIAN_STACK_SIZE = 1000000;
    static const int MEDIAN_STACK_SIZE_INIT = 900000;

    /* MEDIAN HEAP BUCKET */
    typedef struct
    {
        //key
        MedianKey key; // = MEDIAN_KEY_GAURD_VAULE;

        //index to heap
        HeapSize_t index;

        //left or right or no heap  -1,+1,0
        signed char lrHeap;
    } MedianBucket;


    /* MEDIAN */
    typedef struct
    {
        #ifdef MEDIAN_DEBUG
        long long int actualMedianSize=0;
        #endif 

        //median value
        MedianKey median = 0;

        //left max-heap
        MedianBucket *leftHeap[MAX_HEAP];// = {NULL};
        HeapSize_t leftHeapSize = 0;

        //right min-heap
        MedianBucket *rightHeap[MAX_HEAP];// = {NULL};
        HeapSize_t rightHeapSize = 0;

        //heaver heap, right=1, left=-1, equal=0
        signed char heaverHeap = 0;
    } Median;

    /* TABLE BUCKET FOR CELLS */
    typedef struct
    {
        /**
         * key is: long(0...599) and lat(0...599)
         * table index is: (long)*600 + (lat)
         * reverse: long=index/600, lat=index%600
         */
        //TableSize_t key;
        CellIndex_t x;
        CellIndex_t y;

        //number of empty taxis
        EmptyTaxisKey emptyTaxis = 0;

        //median
        Median medianStruct;
        
        //profitability
        ProfitKey profit = 0;

        //index to main heap
        TableSize_t index; //0-359999 or 0-9
        signed char partOf = 0; //0: means nothing, 1:means tops, 2:means  main heap
    } CellBucket;

    
    struct FifoBucket;
    /* HASHTABLE BUCKET FOR TAXIS */
    typedef struct taxi_bucket_typedef
    {
        //taxi key, -1,-1 is guard value
        TaxiHtKey_t key = TAXI_HT_GAURD_VALUE;

        #ifdef Q2_SUBMIT_A
        TaxiHtKey_t key2 = TAXI_HT_GAURD_VALUE;
        #endif

        //pointer to cell
        CellBucket *cellBucket;

        //pointer to fifo bucket
        FifoBucket *fifoBucket;

        //hash table bucket chain
        taxi_bucket_typedef *nextBucket = NULL;
    } TaxiBucket;

    /* FIFO ITEM */
    struct FifoBucket
    {
        FifoBucket *left;
        //FifoBucket *right;
        time_t inputTime;
        Timestamp timestamp;
        TaxiBucket *taxiBucket;
        CellBucket *medianCellBucket;
        MedianBucket *medianBucket;
    };

private:
    /* output file */
    static FILE *outputFile;

    /* TABLE FOR CELLS */
    static CellBucket *cellTable[GRID_SIZE][GRID_SIZE];
    static CellBucket initCellBuckets[CELL_BUCKETS_INIT];
    static void popCellBucket(CellBucket**);
    static TableSize_t cellBucketStackPtr;

    /* TOPS LIST */
    static CellBucket *topsTable[TOPS];
    static signed char topsSize;

    /* ARRAY FOR MAIN MAX-HEAP */
    static CellBucket *mainHeap[CELL_TABLE_SIZE];
    static TableSize_t heapSize;

    /* TOPS AND MAIN HEAP FUNCTION */
    static void updateCellSorting(CellBucket*);

    static void insertCellIntoSorting(CellBucket*);
    static void deleteCellFromSorting(CellBucket*);
    static void updateCellIncreased(CellBucket*);
    static void updateCellDecreased(CellBucket*);

    //tops specific
    static void updateTopsTowardsLeft(CellBucket*);
    static void updateTopsTowardsRight(CellBucket*);
    static void replaceConstTopsLast(CellBucket*);
    //heap specific
    static void insertIntoMainHeap(CellBucket*);
    static void deleteFromMainHeap(CellBucket*);
    static void replaceRootMainHeap(CellBucket*);
    static void heapfyUpMainHeap(CellBucket*);
    static void heapfyDownMainHeap(CellBucket*);


    /* HASHTABLE FOR TAXIS */
    static TaxiBucket taxiTable[TAXI_HT_SIZE];
    static TableSize_t taxiHashFunc(unsigned long long int high/*, long long low*/);

    /* TIME */
    static time_t currentTime;
    static clock_t currentDelay;

    /* MEDIAN */
    static void insertMedian(Median*, MedianKey, MedianBucket**);
    static void deleteMedian(Median*, MedianBucket*);

    //only insert marks lrheap=0 when removing from heap
    //the other functions(which should) don't bother so it won't interfier with other things
    static void insertLeftHeap(Median*, MedianBucket*);
    static void insertRightHeap(Median*, MedianBucket*);
    static void replaceLeftHeapRoot(Median*, MedianBucket*);
    static void replaceRightHeapRoot(Median*, MedianBucket*);
    static void deleteLeftHeap(Median*, MedianBucket*);
    static void deleteRightHeap(Median*, MedianBucket*);
    static void replaceLeftHeapWithGreater(Median*, MedianBucket*, MedianBucket*);
    static void replaceRightHeapWithSmaller(Median*, MedianBucket*, MedianBucket*);
    
    //median recycler
    static MedianBucket *medianBucketStack[MEDIAN_STACK_SIZE];
    static int medianBucketStackPtr;
    static MedianBucket initMedianBuckets[MEDIAN_STACK_SIZE_INIT];
    static void popMedianBucketRecycler(MedianBucket**);
    static void pushMedianBucketRecycler(MedianBucket*);
    
    /* FIFO */
    static FifoBucket *fifoLeftHead;
    static FifoBucket *fifo15minPtr;
    static FifoBucket *fifoRightTail;
    static void fifoPushFront(FifoBucket*);
    static void fifoPopBackAllExpired();
    //fifo recycler
    static FifoBucket *fifoBucketStack[FIFO_STACK_SIZE];
    static int fifoBucketStackPtr;
    static FifoBucket initFifoBuckets[FIFO_STACK_SIZE_INIT];
    static void popFifoBucketRecycler(FifoBucket **);
    static void pushFifoBucketRecycler(FifoBucket *);

    /* TOPS FLAG */
    static bool topsChanged;

    static void dataStructQ2Init(); //initialization

    static void cleanUp(); //clean up in the end

    /* PRINT OUTPUT STREAM */
    #ifdef Q2_SUBMIT_A
    static void printOutput(Timestamp, time_t, short int, Record*);
    #else
    static void printOutput(Timestamp, time_t, short int);
    #endif


public:
    /* OBJECT FLAG */
    static int errorFlag;

    /* API */
    //must be called from main thread before multithreading
    static void dataStructQ2InitFast(char*);

    //main query 2 thread
    static void *query2_thread_main(void *);

    //output thread if defined
    #ifdef Q2_DO_OUTPUT_THREAD
    static void *outputThread_main(void *);
    #endif
    /* END OF API */



    /* OUTPUT THREAD */
    #ifdef Q2_DO_OUTPUT_THREAD
    typedef int OutputFifoRecycler_t;

    typedef struct output_fifo_typedef
    {
        output_fifo_typedef *prev;
        CellIndex_t x[TOPS];
        CellIndex_t y[TOPS];        
        EmptyTaxisKey emptyTaxis[TOPS];
        MedianKey median[TOPS];
        ProfitKey profit[TOPS];
        signed char tops;
        Timestamp timestamp;
        time_t stime;
        short int offset;
        clock_t currentDelay;
        Record *rec;
    } OutputFifoItem;

    //FIFO VARIABLES
    static OutputFifoItem *outputFifoLeftHead;
    static OutputFifoItem *outputFifoRightTail;
    //END OF FIFO VARIABLES

    //OUTPUT FIFO RECYCLER VARIABLES
    static const OutputFifoRecycler_t OUTPUT_FIFO_RECYCLER_STACK_SIZE = 15000;
    static OutputFifoItem *ouputFifoRecyclerStack[OUTPUT_FIFO_RECYCLER_STACK_SIZE];
    static OutputFifoRecycler_t ouputFifoRecyclerStackPtr;
    static pthread_spinlock_t fifoRecyclerStackLock;
    static OutputFifoItem initOutputFifoItems[OUTPUT_FIFO_RECYCLER_STACK_SIZE];
    //END OF OUTPUT FIFO RECYCLER VARIABLES

    //SYNCRO VARIABLES
    static pthread_mutex_t outputMutex;
    static pthread_cond_t outputCond;
    static bool cleanUpCalled;
    static bool outputThreadBlocked;
    //END OF SYNCRO VARIABLES

    static void outputThreadCleanUp();

    static void pushOutputFifoItem(OutputFifoItem*);

    static void popOutputFifoItemRecycler(OutputFifoItem **);
    static void pushOutputFifoItemRecycler(OutputFifoItem *);
    //void pushOutputStack();
    #endif




    //debug and timing
    #ifdef Q2_COMPILE_DEBUG_CODE
    static void printOuputMessages();
    
    #ifdef Q2_TIMING
    static clock_t Q2_acc;
    static long long int Q2_counter;
    static long long int Q2_counter_valid;
    static long long int Q2_counter_print;
    #endif

    #endif
};
#endif // DATASTRUCTQ2_H_
