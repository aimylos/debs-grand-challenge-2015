#ifndef DATASTRUCTQ1_H_
#define DATASTRUCTQ1_H_

#include "data.h"
#include "parameters.h"

#include <time.h>
#include <stdlib.h>
#include <stdio.h>

class DataStructQ1
{
public:
    //types
    typedef RouteId HtKey_t; //long long unsigned int HtKey_t;
    typedef short int HtValue_t;
    typedef int HtTableSize_t;

    //constants
    static const HtTableSize_t HT_SIZE = 1000000;
    static const signed char TOPS = 10;
    static const HtValue_t COUNTER_TABLE_SIZE = 1000;
    static const HtKey_t HT_GUARD_VALUE = 0; //-1;
    static const HtValue_t COUNTER_INIT = 0;
    static const int FIFO_STACK_SIZE = 50000; //50000; //80000;
    static const int FIFO_STACK_SIZE_INIT = 40000; //50000; //80000;

    /* HASHTABLE - LIST ITEM */
    typedef struct bucket_typedef
    {
        //(key,value) pair
        HtKey_t key = HT_GUARD_VALUE;
        RouteCells cells; //increased by 1, ready to be printed

        HtValue_t value = COUNTER_INIT;

        // hashtable bucket chain
        bucket_typedef *nextBucket = NULL;

        //list bucket linking
        bucket_typedef *listNext = NULL;
        bucket_typedef *listPrev = NULL;

        //flag for HT_TOPS first buckets in list
        bool inTops = false;
    } Bucket;

    /* FIFO ITEM */
    typedef struct fifo_bucket_typedef
    {
        fifo_bucket_typedef *left;
        //fifo_bucket_typedef *right;
        time_t inputTime;
        Timestamp timestamp;
        Bucket *counterBucket;
    } FifoBucket;

private:
    /* output file */
    static FILE *outputFile;

    /* HASHTABLE */
    static Bucket table[HT_SIZE];
    static HtTableSize_t hashFunc(RouteId key);
    //void forceIncCounter(HT_SIZE_TYPE hashed_key, HT_KEY_TYPE _key, time_t expire_time_for_FIFO_call);
    //Bucket* getListOutOfBoundHead();
    //bool* getTopsChangedPtr();


    /* LIST */
    //pointers
    static Bucket listHead;
    static Bucket listTail;
    static Bucket *lastTop;

    //flags
    //static bool getTopsChangedPtred;
    static bool topsReached;
    static bool topsChanged;
    //no delete no size decrease, will go up to HT_TOPS no more
    static HtValue_t listSize;
    //counter sets: [i][0]->counter leader,  [i][1]->counter tail
    static Bucket* leaders[COUNTER_TABLE_SIZE][2];

    static void inc_bucket_update_list(Bucket*);
    static void dec_counter_update_list(Bucket*);
    static void push_back_list(Bucket*);
    static void close_gap(Bucket*);
    static void squeeze_new_leader(Bucket *prevBucket, Bucket *bucket);
    static void update_last_top_incd(Bucket *prevBucket, Bucket *bucket);
    static void update_last_top_decd(Bucket *prevBucket, Bucket *bucket);


    /* FIFO */
    static FifoBucket *fifoLeftHead;
    static FifoBucket *fifoRightTail;
    static void fifoPushFront(FifoBucket*);
    static void fifoPopBackAllExpired();
    //fifo recycler
    static FifoBucket *fifoBucketStack[FIFO_STACK_SIZE];
    static int fifoBucketStackPtr;
    static FifoBucket initFifoBuckets[FIFO_STACK_SIZE_INIT];
    static void popFifoBucketRecycler(FifoBucket **);
    static void pushFifoBucketRecycler(FifoBucket *);
    //last update on time
    static time_t currentTime;
    static clock_t currentDelay;


    /* Full Initialization */
    static void dataStructQ1Init();

    /* Clean up, called by main thread on exit */
    static void cleanUp();

    /* PRINT OUTPUT STREAM */
    #ifdef Q1_SUBMIT_A
    static void printOutput(Timestamp, time_t, short int, Record*);
    #else
    static void printOutput(Timestamp, time_t, short int);
    #endif

public:
    /* OBJECT FLAG */
    static int errorFlag;


    /* API */
    //must be called from main thread before multithreading
    static void dataStructQ1InitFast(char *);
    
    //main query 1 thread
    static void *query1_thread_main(void *);
    
    //output thread if defined
    #ifdef Q1_DO_OUTPUT_THREAD
    static void *outputThread_main(void *);
    #endif
    /* END OF API */


    /* OUTPUT THREAD */
    #ifdef Q1_DO_OUTPUT_THREAD
    typedef int OutputFifoRecycler_t;

    typedef struct output_fifo_typedef
    {
        output_fifo_typedef *prev;
        RouteCells routeCells[TOPS];
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
    static const OutputFifoRecycler_t OUTPUT_FIFO_RECYCLER_STACK_SIZE = 10000;
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
    #endif


    //debug and timing
    #ifdef Q1_COMPILE_DEBUG_CODE
    static void printOuputMessages();

    #ifdef Q1_TIMING
    static clock_t Q1_acc;
    static long long int Q1_counter;
    static long long int Q1_counter_valid;
    static long long int Q1_counter_print;
    #endif

    //other debug functions
    // static void print_table();
    // static void print_list();
    // static void print_tops(Record *);

    #endif
};
#endif // DATASTRUCTQ1_H_
