#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

#include "data.h"
#include "parameters.h"

#ifdef Q1_SUBMIT_A
#include "../src/query1/dataStructQ1.h"
#include "parser/parser.h"
#else
#include "query1/dataStructQ1.h"
#include "lineparser/parser_includes.h"
#include "util/synchronization.h"
#endif



//static variables
int DataStructQ1::errorFlag;
FILE *DataStructQ1::outputFile;
DataStructQ1::Bucket DataStructQ1::table[DataStructQ1::HT_SIZE];
DataStructQ1::Bucket DataStructQ1::listHead;
DataStructQ1::Bucket DataStructQ1::listTail;
DataStructQ1::Bucket *DataStructQ1::lastTop;
bool DataStructQ1::topsChanged;
bool DataStructQ1::topsReached;
DataStructQ1::HtValue_t DataStructQ1::listSize;
DataStructQ1::Bucket* DataStructQ1::leaders[DataStructQ1::COUNTER_TABLE_SIZE][2];
DataStructQ1::FifoBucket *DataStructQ1::fifoLeftHead;
DataStructQ1::FifoBucket *DataStructQ1::fifoRightTail;
time_t DataStructQ1::currentTime;
clock_t DataStructQ1::currentDelay;
DataStructQ1::FifoBucket *DataStructQ1::fifoBucketStack[DataStructQ1::FIFO_STACK_SIZE];
int DataStructQ1::fifoBucketStackPtr;
DataStructQ1::FifoBucket DataStructQ1::initFifoBuckets[DataStructQ1::FIFO_STACK_SIZE_INIT];

#ifdef Q1_DO_OUTPUT_THREAD
DataStructQ1::OutputFifoItem *DataStructQ1::ouputFifoRecyclerStack[DataStructQ1::OUTPUT_FIFO_RECYCLER_STACK_SIZE];
DataStructQ1::OutputFifoRecycler_t DataStructQ1::ouputFifoRecyclerStackPtr;
pthread_spinlock_t DataStructQ1::fifoRecyclerStackLock;
DataStructQ1::OutputFifoItem DataStructQ1::initOutputFifoItems[DataStructQ1::OUTPUT_FIFO_RECYCLER_STACK_SIZE];
pthread_mutex_t DataStructQ1::outputMutex;
pthread_cond_t DataStructQ1::outputCond;
bool DataStructQ1::cleanUpCalled;
bool DataStructQ1::outputThreadBlocked;
DataStructQ1::OutputFifoItem *DataStructQ1::outputFifoLeftHead;
DataStructQ1::OutputFifoItem *DataStructQ1::outputFifoRightTail;
#endif

#ifdef Q1_TIMING
clock_t DataStructQ1::Q1_acc = 0;
long long int DataStructQ1::Q1_counter = 0;
long long int DataStructQ1::Q1_counter_valid = 0;
long long int DataStructQ1::Q1_counter_print = 0;
#endif


void DataStructQ1::dataStructQ1InitFast(char *filename)
{
    //clear error flag
    errorFlag = 0;

    //open file
    if (filename != NULL) outputFile = fopen(filename, "w");
    else outputFile = fopen("output1", "w");

    #ifdef Q1_DO_OUTPUT_THREAD
    pthread_spin_init(&fifoRecyclerStackLock, PTHREAD_PROCESS_PRIVATE);
    pthread_mutex_init(&outputMutex, NULL);
    pthread_cond_init (&outputCond, NULL);
    cleanUpCalled = false;
    outputThreadBlocked = false;
    outputFifoLeftHead = NULL;
    outputFifoRightTail = NULL;
    #endif
}


void DataStructQ1::dataStructQ1Init()
{
    /* LIST */
    //initialize leaders to NULL
    memset(leaders, 0, sizeof(leaders[0][0])*COUNTER_TABLE_SIZE * 2);

    //outside limits values
    listHead.value = COUNTER_TABLE_SIZE;
    listTail.value = -1;

    //put head and tail at leaders' table borders
    leaders[COUNTER_TABLE_SIZE - 1][0] = &listHead;
    leaders[COUNTER_TABLE_SIZE - 1][1] = &listHead;
    leaders[0][0] = &listTail;
    leaders[0][1] = &listTail;

    //link head and tail
    listHead.listPrev = &listTail;
    listTail.listNext = &listHead;

    //init other variables
    lastTop = NULL;
    topsChanged = false;
    topsReached = false;
    listSize = 0;

    /* FIFO */
    currentTime = 0;
    currentDelay = 0;
    fifoLeftHead = NULL;
    fifoRightTail = NULL;
    //push staticly allocated fifo buckets into stack
    //this is cpu time that must be overlapped by other first io wait
    for (fifoBucketStackPtr = 0; fifoBucketStackPtr < FIFO_STACK_SIZE_INIT; fifoBucketStackPtr++)
    {
        fifoBucketStack[fifoBucketStackPtr] = &(initFifoBuckets[fifoBucketStackPtr]);
    }
    fifoBucketStackPtr--; //points to the tops of the statck

    #ifdef Q1_DO_OUTPUT_THREAD
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


void *DataStructQ1::query1_thread_main(void *)
{
    //init query 1 data struct
    dataStructQ1Init();

    //temp vars
    Record *rec;

    int counter = 0;

    while (true)
    {
        /*********************************/
        /*     Get record from parser    */
        /*********************************/
        #ifdef Q1_SUBMIT_A
        rec = Parser::getRecord(Q1_ID);
        if(rec == NULL)
          break;
        #else
        
        rec = RecordBuffer::getRecordQ1();
        counter++;

        //printf("%016llX\n", rec->taxi.high);
        if (!rec->valid)
        {
          if (rec->state & RECORD_EOF)
          {
            //printf("Q1 counter: %i\n", counter);
            break;
          }
          continue;
        }
        #endif
        /*********************************/
        /* End of get record from parser */
        /*********************************/

        #ifdef Q1_TIMING
        clock_t Q1_start = clock();
        #endif

        /* update current time */
        currentTime = rec->dropoffTime;
        currentDelay = rec->startClockTick;

        /* pop all expired given the new current time */
        fifoPopBackAllExpired();

        /* check if route is valid */
        if (rec->route300.id == 0)
        {
            #ifdef Q1_TIMING
            Q1_acc += clock() - Q1_start;
            Q1_counter++;
            #endif
            continue;
        }

        /* find for insert into hashtable */
        Bucket *tempBucket = &(table[hashFunc(rec->route300.id)]);

        //find correct bucket and update
        while (1)
        {
            if (tempBucket->key == rec->route300.id)
            {
                //key exists
                //@@@@@ increase value and update list
                inc_bucket_update_list(tempBucket);
                break;
            }
            else if (tempBucket->nextBucket == NULL)
            {
                //key doesn't exist, create bucket if needed
                if ( tempBucket->key != HT_GUARD_VALUE )
                {
                    tempBucket = (tempBucket->nextBucket = new Bucket);
                }
                //init new bucket and put infront of first counter=1 of the list
                tempBucket->key = rec->route300.id;
                //put cells increased by 1
                tempBucket->cells.pickX = rec->route300.cells.pickX + 1;
                tempBucket->cells.pickY = rec->route300.cells.pickY + 1;
                tempBucket->cells.dropX = rec->route300.cells.dropX + 1;
                tempBucket->cells.dropY = rec->route300.cells.dropY + 1;
                tempBucket->value = 1;
                //@@@@@ put in front of leader counter=1
                push_back_list(tempBucket);
                break;
            }
            else
            {
                tempBucket = tempBucket->nextBucket;
            }
        }

        /* push front into fifo */
        FifoBucket *fifoBucket;
        popFifoBucketRecycler(&fifoBucket);
        fifoBucket->counterBucket = tempBucket;
        fifoBucket->inputTime = currentTime;
        fifoBucket->timestamp = rec->dropoffTimestamp;
        fifoPushFront(fifoBucket);

        /* print output if needed */
        if (topsChanged)
        {
            //reset tops boolean
            topsChanged = false;

            //print output stream
            #ifdef Q1_OUTPUT_STREAM
            #ifdef Q1_SUBMIT_A
            //record has all information needed
            printOutput( rec->dropoffTimestamp, 0, 0, rec);
            #else
            //offset is negative for pickup time
            printOutput(rec->dropoffTimestamp, currentTime, -(rec->duration));
            #endif
            #endif
        }

        #ifdef Q1_TIMING
        Q1_acc += clock() - Q1_start;
        Q1_counter++;
        Q1_counter_valid++;
        #endif
    }

    cleanUp();
    pthread_exit(NULL);
}

void DataStructQ1::cleanUp()
{
    /* pop all expired given the new current time */
    //currentTime += 3600;
    //fifoPopBackAllExpired();

    #ifdef Q1_DO_OUTPUT_THREAD
    pthread_mutex_lock(&outputMutex);
    //cout<<"cleanup set to true"<<endl;
    cleanUpCalled = true;
    pthread_cond_signal(&outputCond);
    pthread_mutex_unlock(&outputMutex);
    #else
    //close output file
    fclose(outputFile);  
    #endif

    //printf("Q1 main thread exiting...\n");
}

inline DataStructQ1::HtTableSize_t DataStructQ1::hashFunc(RouteId key)
{
    //return _key%table_size;
    key = (1485500463 * key + 1179660437) % 2147483647;
    key = (key < 0) ? -key : key;
    return key % HT_SIZE;
}

/** HASHTABLE AND LIST DEFINITIONS **/
inline void DataStructQ1::inc_bucket_update_list(Bucket *bucket)
{
    //swap needed
    if ( (bucket->listNext)->value <= (bucket->value + 1) )
    {
        //close gap
        close_gap(bucket);

        //increase counter
        bucket->value++;

        //new leader is not NULL
        if ( leaders[bucket->value][0] != NULL )
        {
            //update last top if needed
            update_last_top_incd(leaders[bucket->value][0], bucket);
            //squeeze
            squeeze_new_leader(leaders[bucket->value][0], bucket);
        }
        //new leader is NULL
        //that means there is still a leader of bucket's old value since swap was needed
        else
        {
            //update last top if needed
            update_last_top_incd(leaders[bucket->value - 1][0], bucket);
            //squeeze
            squeeze_new_leader(leaders[bucket->value - 1][0], bucket);
        }
        return;
    }
    //else swap not needed
    else
    {
        //swap not needed means that
        //bucket is leader of old counter and new counter is NULL

        //bucket is the only one in this counter
        if (leaders[bucket->value][0] == leaders[bucket->value][1])
        {
            //update leader and tail
            leaders[bucket->value][0] = NULL;
            leaders[bucket->value][1] = NULL;
        }
        //else there's at least one more bucket in this counter
        else
        {
            leaders[bucket->value][0] = bucket->listPrev;
        }

        //increase counter
        bucket->value++;

        //update new leader and tail
        leaders[bucket->value][0] = bucket;
        leaders[bucket->value][1] = bucket;
        return;
    }
}

inline void DataStructQ1::dec_counter_update_list(Bucket *bucket)
{
    //check if decrease is possible
    if (bucket->value == 0) return;

    //swap needed
    if ( (bucket->listPrev)->value > (bucket->value - 1) )
    {
        //close gap
        close_gap(bucket);

        //decrease counter
        bucket->value--;

        //update last top if needed
        update_last_top_decd( (leaders[bucket->value + 1][1])->listPrev, bucket );

        //there's a tail for sure in bucket's old counter else swap is not needed
        squeeze_new_leader( (leaders[bucket->value + 1][1])->listPrev, bucket );
        return;
    }
    else
    {
        //swap not needed means that bucket is the tail of it's counter
        //and it justs needs to be the leader of the previous counter

        //this bucket is the only in this counter
        if (leaders[bucket->value][0] == leaders[bucket->value][1])
        {
            //update leader and tail
            leaders[bucket->value][0] = NULL;
            leaders[bucket->value][1] = NULL;
        }
        //else there's at least one more bucket in this counter
        else
        {
            leaders[bucket->value][1] = bucket->listNext;
        }

        //decrease counter
        bucket->value--;

        //update new leader and tail
        //counter is empty
        if (leaders[bucket->value][0] == NULL)
        {
            leaders[bucket->value][0] = bucket;
            leaders[bucket->value][1] = bucket;
        }
        //there's at least one bucket in counter
        else
        {
            leaders[bucket->value][0] = bucket;
        }
        return;
    }
}


inline void DataStructQ1::push_back_list(Bucket *bucket)
{
    //could also put it at list_end and call update_list_incdbucket()

    //if counter 1 has a leader
    if (leaders[1][0] != NULL)
    {
        squeeze_new_leader(leaders[1][0], bucket);
    }
    //else counter 0 has at least one leader for sure (list_tail)
    else
    {
        squeeze_new_leader(leaders[0][0], bucket);
    }

    //check for tops
    if (!topsReached)
    {
        //do this for TOPS first buckets
        listSize++;
        if (listSize >= TOPS) topsReached = true;

        bucket->inTops = true;
        lastTop = listTail.listNext;

        topsChanged = true;
    }
    else if ( (bucket->listPrev)->inTops )
    {
        bucket->inTops = true;
        lastTop->inTops = false;
        lastTop = lastTop->listNext;

        topsChanged = true;
    }
}

inline void DataStructQ1::close_gap(Bucket *bucket)
{
    //link gap
    (bucket->listNext)->listPrev = bucket->listPrev;
    (bucket->listPrev)->listNext = bucket->listNext;

    //update leader and tail
    //bucket is the only bucket in this counter
    if (leaders[bucket->value][0] == leaders[bucket->value][1])
    {
        leaders[bucket->value][0] = NULL;
        leaders[bucket->value][1] = NULL;
    }
    //bucket was the leader in this counter
    else if (leaders[bucket->value][0] == bucket)
    {
        leaders[bucket->value][0] = bucket->listPrev;
    }
    //bucket was the tail in this counter
    else if (leaders[bucket->value][1] == bucket)
    {
        leaders[bucket->value][1] = bucket->listNext;
    }
}


inline void DataStructQ1::squeeze_new_leader(Bucket *prevBucket, Bucket *bucket)
{
    //link bucket
    bucket->listPrev = prevBucket;
    bucket->listNext = prevBucket->listNext;

    //link prevbucket and bucket->list_next
    prevBucket->listNext = bucket;
    (bucket->listNext)->listPrev = bucket;

    //update leader and tail
    //prevbucket was leader in this counter
    if (bucket->value == prevBucket->value)
        //if(leaders[bucket->value][0] == prevbucket)
    {
        leaders[bucket->value][0] = bucket;
    }
    //else there's no leader in this counter
    else
    {
        leaders[bucket->value][0] = bucket;
        leaders[bucket->value][1] = bucket;
    }
}


inline void DataStructQ1::update_last_top_incd(Bucket *prevBucket, Bucket *bucket)
{
    //bucket is in tops
    if (bucket->inTops)
    {
        //bucket is last top
        if (bucket == lastTop)
        {
            lastTop = bucket->listNext;
        }
        topsChanged = true;
    }
    //bucket is not in tops
    //prevbucket is in tops
    else if (prevBucket->inTops)
    {
        bucket->inTops = true;
        lastTop->inTops = false;

        //move last top towards head
        //last top is prevbucket
        if (lastTop == prevBucket)
        {
            lastTop = bucket;
        }
        //else last top is not prevbucket
        else
        {
            lastTop = lastTop->listNext;
        }
        topsChanged = true;
    }
}


inline void DataStructQ1::update_last_top_decd(Bucket *prevBucket, Bucket *bucket)
{
    //prev bucket is in tops
    if (prevBucket->inTops)
    {
        topsChanged = true;
    }
    //bucket is in tops
    else if (bucket->inTops)
    {
        //prevbucket is the previous of last top
        if (prevBucket->listNext == lastTop)
        {
            //new last top is bucket
            lastTop = bucket;
        }
        //else just move last top towards tail
        else
        {
            bucket->inTops = false;
            //move last top towards tail
            lastTop = lastTop->listPrev;
            lastTop->inTops = true;
        }
        topsChanged = true;
    }
}
/** END OF HASHTABLE AND LIST DEFINITIONS **/


/** FIFO DEFINITIONS **/
inline void DataStructQ1::fifoPushFront(FifoBucket *bucket)
{
    //link to FIFO
    if (fifoLeftHead == NULL)
    {
        //empty FIFO
        fifoLeftHead = fifoRightTail = bucket;
        bucket->left = NULL;
        //bucket->right=NULL;
    }
    else
    {
        fifoLeftHead->left = bucket;
        //bucket->right = fifoLeftHead;
        bucket->left = NULL;
        fifoLeftHead = bucket;
    }
}

inline void DataStructQ1::fifoPopBackAllExpired()
{
    //check for empty list
    if (fifoRightTail == NULL) return;

    //pop all expired
    while (fifoRightTail->inputTime + 1800 <= currentTime)
    {
        //decrease counter
        dec_counter_update_list(fifoRightTail->counterBucket);

        /* print output if needed */
        if (topsChanged)
        {
            //reset tops boolean
            topsChanged = false;

            //print output stream
            #ifdef Q1_OUTPUT_STREAM
            
            #ifdef Q1_SUBMIT_A
            printOutput(fifoRightTail->timestamp, fifoRightTail->inputTime, 1800, NULL);
            #else
            //offset is negative for pickup time
            printOutput(fifoRightTail->timestamp, fifoRightTail->inputTime, 1800);
            #endif            
            
            #endif
        }

        //remove FIFO tail
        //if fifo has only 1 item left
        if (fifoRightTail->left == NULL)
        {
            //now fifo will be empty
            pushFifoBucketRecycler(fifoLeftHead);
            fifoRightTail = fifoLeftHead = NULL;
            return;
        }
        else
        {
            FifoBucket *temp = fifoRightTail;
            fifoRightTail = fifoRightTail->left;
            pushFifoBucketRecycler(temp);
        }
    }
    return;
}

inline void DataStructQ1::popFifoBucketRecycler(FifoBucket **returnPtr)
{
    //static int mallocs=0;
    //case of NOT empty stack
    if (fifoBucketStackPtr >= 0)
    {
        *returnPtr = fifoBucketStack[fifoBucketStackPtr];
        fifoBucketStackPtr--;
        return;
    }
    //case of empty stack
    else
    {
        *returnPtr = (FifoBucket *)malloc(sizeof(FifoBucket));
        //cout << "fifo MALLOCED" <<endl;
        //cout<<++mallocs<<endl;
        return;
    }
}

inline void DataStructQ1::pushFifoBucketRecycler(FifoBucket *bucket)
{
    //case of NOT full stack
    if (fifoBucketStackPtr < FIFO_STACK_SIZE - 1)
    {
        fifoBucketStackPtr++;
        fifoBucketStack[fifoBucketStackPtr] = bucket;
        return;
    }
    //case of full stack and bucket is not part of initFifoBuckets
    //so free can be called
    else if ( bucket < initFifoBuckets || bucket >= initFifoBuckets + FIFO_STACK_SIZE_INIT)
    {
        //cout << "fifo FREED" <<endl;
        free(bucket);
        return;
    }
}
/** END OF FIFO DEFINITIONS **/






/****************************************************************/
/********************* DEBUG AND TIMING CODE ********************/
/****************************************************************/
#ifdef Q1_COMPILE_DEBUG_CODE
void DataStructQ1::printOuputMessages()
{
    #ifdef Q1_TIMING
    printf("\nQ1 avg time in sec: %lf\n", ((double)Q1_acc / Q1_counter) / CLOCKS_PER_SEC);
    printf("Q1 avg time in tics: %lf\n", (double)Q1_acc / Q1_counter);
    printf("Q1 whole time in tics %ld\n", Q1_acc);
    printf("Q1 whole time in secs %lf\n", (double)Q1_acc / CLOCKS_PER_SEC);
    printf("Q1 got %lld records of which %lld where valid.\n", Q1_counter, Q1_counter_valid);
    printf("Q1 printed %lld times.\n", Q1_counter_print);
    #endif
}
#endif
/****************************************************************/
/***************** END OF DEBUG AND TIMING CODE *****************/
/****************************************************************/
