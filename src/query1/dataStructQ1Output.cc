#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "data.h"
#include "parameters.h"
#include "util/synchronization.h"

#include "query1/dataStructQ1.h"
#include "parser/mktime.h"
#include "lineparser/parser_includes.h"
#include "util/synchronization.h"


/** PRINT OUTPUT STREAM FUNCTION **/
inline void getTimestamps(Timestamp *ts, Timestamp timestamp, time_t stime, short int offset)
{
    *ts = timestamp;
    stime += offset;

    if (offset > 0)
    {
        //case of month increase
        if (stime >= mkTime[timestamp.year * 12 + timestamp.month + 1])
        {
            stime = stime - mkTime[timestamp.year * 12 + timestamp.month + 1];
            ts->month += 1;
            if (ts->month >= 12) {ts->month = 0; ts->year += 1;}
        }
        //case no month increase
        else
        {
            stime = stime - mkTime[timestamp.year * 12 + timestamp.month];
        }
    }
    else if (offset < 0)
    {
        //case of month decrease
        if (stime < mkTime[timestamp.year * 12 + timestamp.month])
        {
            stime = stime - mkTime[timestamp.year * 12 + timestamp.month - 1];
            ts->month -= 1;
            if (ts->month < 0) {ts->month = 0; ts->year -= 1;}
        }
        //case no month decrease
        else
        {
            stime = stime - mkTime[timestamp.year * 12 + timestamp.month];
        }
    }
    else
    {
        //offset is zero
        return;
    }

    ts->day = stime / 86400;
    stime %= 86400;
    ts->hour = stime / 3600;
    stime %= 3600;
    ts->min = stime / 60;
    ts->sec = stime % 60;
}


void DataStructQ1::printOutput(Timestamp timestamp, time_t stime, short int offset)
{
#ifdef Q1_DO_OUTPUT_THREAD

    OutputFifoItem *item;
    popOutputFifoItemRecycler(&item);

    item->timestamp = timestamp;
    item->stime = stime;
    item->offset = offset;
    item->currentDelay = currentDelay;
    item->tops = 0;
    Bucket *bucket = listHead.listPrev;

    //get tops
    if (bucket != NULL)
    {
        while ( bucket->listPrev != NULL && item->tops < TOPS )
        {
            item->routeCells[item->tops] = bucket->cells;
            bucket = bucket->listPrev;
            item->tops++;
        }
    }
    //push work to output fifo
    pushOutputFifoItem(item);

#else

    static time_t lastTime = 0;
    static Timestamp lastTimestamp;

    //calculate timestamp
    Timestamp ts;
    if (lastTime == stime + offset) ts = lastTimestamp;
    else
    {
        getTimestamps(&ts, timestamp, stime, offset);
        lastTime = stime + offset;
        lastTimestamp = ts;
    }

    short int i = 0;
    Bucket *topsPtr[TOPS];
    Bucket *bucket = listHead.listPrev;

    //get tops
    if (bucket != NULL)
    {
        while ( bucket->listPrev != NULL && i < TOPS )
        {
            topsPtr[i] = bucket;
            bucket = bucket->listPrev;
            i++;
        }
    }

    //print output to file
    //case of all tops
    if ( i >= TOPS )
    {
        //only expiration timestamp
        if (offset > 0)
        {
            fprintf(outputFile, "%hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,"
                    "%hd.%hd,%hd.%hd,"
                    "%hd.%hd,%hd.%hd,"
                    "%hd.%hd,%hd.%hd,"
                    "%hd.%hd,%hd.%hd,"
                    "%hd.%hd,%hd.%hd,"
                    "%hd.%hd,%hd.%hd,"
                    "%hd.%hd,%hd.%hd,"
                    "%hd.%hd,%hd.%hd,"
                    "%hd.%hd,%hd.%hd,"
                    "%hd.%hd,%hd.%hd,%li\n",
                    ts.year + 1970, ts.month + 1, ts.day, ts.hour, ts.min, ts.sec,

                    topsPtr[0]->cells.pickX, topsPtr[0]->cells.pickY,
                    topsPtr[0]->cells.dropX, topsPtr[0]->cells.dropY,

                    topsPtr[1]->cells.pickX, topsPtr[1]->cells.pickY,
                    topsPtr[1]->cells.dropX, topsPtr[1]->cells.dropY,

                    topsPtr[2]->cells.pickX, topsPtr[2]->cells.pickY,
                    topsPtr[2]->cells.dropX, topsPtr[2]->cells.dropY,

                    topsPtr[3]->cells.pickX, topsPtr[3]->cells.pickY,
                    topsPtr[3]->cells.dropX, topsPtr[3]->cells.dropY,

                    topsPtr[4]->cells.pickX, topsPtr[4]->cells.pickY,
                    topsPtr[4]->cells.dropX, topsPtr[4]->cells.dropY,

                    topsPtr[5]->cells.pickX, topsPtr[5]->cells.pickY,
                    topsPtr[5]->cells.dropX, topsPtr[5]->cells.dropY,

                    topsPtr[6]->cells.pickX, topsPtr[6]->cells.pickY,
                    topsPtr[6]->cells.dropX, topsPtr[6]->cells.dropY,

                    topsPtr[7]->cells.pickX, topsPtr[7]->cells.pickY,
                    topsPtr[7]->cells.dropX, topsPtr[7]->cells.dropY,

                    topsPtr[8]->cells.pickX, topsPtr[8]->cells.pickY,
                    topsPtr[8]->cells.dropX, topsPtr[8]->cells.dropY,

                    topsPtr[9]->cells.pickX, topsPtr[9]->cells.pickY,
                    topsPtr[9]->cells.dropX, topsPtr[9]->cells.dropY,
                    getFastTimestamp() - currentDelay);
        }
        //both pick and dropoff timestamps
        else
        {
            fprintf(outputFile, "%hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,"
                    "%hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,"
                    "%hd.%hd,%hd.%hd,"
                    "%hd.%hd,%hd.%hd,"
                    "%hd.%hd,%hd.%hd,"
                    "%hd.%hd,%hd.%hd,"
                    "%hd.%hd,%hd.%hd,"
                    "%hd.%hd,%hd.%hd,"
                    "%hd.%hd,%hd.%hd,"
                    "%hd.%hd,%hd.%hd,"
                    "%hd.%hd,%hd.%hd,"
                    "%hd.%hd,%hd.%hd,%li\n",
                    ts.year + 1970, ts.month + 1, ts.day, ts.hour, ts.min, ts.sec,
                    timestamp.year + 1970, timestamp.month + 1, timestamp.day,
                    timestamp.hour, timestamp.min, timestamp.sec,

                    topsPtr[0]->cells.pickX, topsPtr[0]->cells.pickY,
                    topsPtr[0]->cells.dropX, topsPtr[0]->cells.dropY,

                    topsPtr[1]->cells.pickX, topsPtr[1]->cells.pickY,
                    topsPtr[1]->cells.dropX, topsPtr[1]->cells.dropY,

                    topsPtr[2]->cells.pickX, topsPtr[2]->cells.pickY,
                    topsPtr[2]->cells.dropX, topsPtr[2]->cells.dropY,

                    topsPtr[3]->cells.pickX, topsPtr[3]->cells.pickY,
                    topsPtr[3]->cells.dropX, topsPtr[3]->cells.dropY,

                    topsPtr[4]->cells.pickX, topsPtr[4]->cells.pickY,
                    topsPtr[4]->cells.dropX, topsPtr[4]->cells.dropY,

                    topsPtr[5]->cells.pickX, topsPtr[5]->cells.pickY,
                    topsPtr[5]->cells.dropX, topsPtr[5]->cells.dropY,

                    topsPtr[6]->cells.pickX, topsPtr[6]->cells.pickY,
                    topsPtr[6]->cells.dropX, topsPtr[6]->cells.dropY,

                    topsPtr[7]->cells.pickX, topsPtr[7]->cells.pickY,
                    topsPtr[7]->cells.dropX, topsPtr[7]->cells.dropY,

                    topsPtr[8]->cells.pickX, topsPtr[8]->cells.pickY,
                    topsPtr[8]->cells.dropX, topsPtr[8]->cells.dropY,

                    topsPtr[9]->cells.pickX, topsPtr[9]->cells.pickY,
                    topsPtr[9]->cells.dropX, topsPtr[9]->cells.dropY,
                    getFastTimestamp() - currentDelay);
        }
    }
    //case of less that TOPS tops
    else
    {
        char buf[450];
        short int s;

        if (offset > 0)
        {
            s = sprintf(buf, "%02hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,",
                        ts.year + 1970, ts.month + 1, ts.day, ts.hour, ts.min, ts.sec);
        }
        else
        {
            s = sprintf(buf, "%hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,"
                        "%hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,",
                        ts.year + 1970, ts.month + 1, ts.day, ts.hour, ts.min, ts.sec,
                        timestamp.year + 1970, timestamp.month + 1, timestamp.day,
                        timestamp.hour, timestamp.min, timestamp.sec);
        }

        for (short int j = 0; j < i; j++)
        {
            s += sprintf(buf + s, "%hd.%hd,%hd.%hd,",
                         topsPtr[j]->cells.pickX, topsPtr[j]->cells.pickY,
                         topsPtr[j]->cells.dropX, topsPtr[j]->cells.dropY);
        }

        //print nulls
        while ( i < TOPS )
        {
            *(buf + s++) = 'N';
            *(buf + s++) = 'U';
            *(buf + s++) = 'L';
            *(buf + s++) = 'L';
            *(buf + s++) = ',';
            i++;
        }
        buf[s - 1] = '\0';

        fprintf(outputFile, "%s,%li\n", buf, getFastTimestamp() - currentDelay);
    }

    #ifdef Q1_TIMING
    Q1_counter_print++;
    #endif
#endif
}






#ifdef Q1_DO_OUTPUT_THREAD
void DataStructQ1::outputThreadCleanUp()
{
    //close output file
    fclose(outputFile);

    //printf("Q1 output thread exiting...\n");
}


void *DataStructQ1::outputThread_main(void *)
{
    OutputFifoItem *item;
    time_t lastTime = 0;
    Timestamp lastTimestamp;
    Timestamp ts;

    while (1)
    {
        pthread_mutex_lock(&outputMutex);
        //check if it's time to stop
        if (cleanUpCalled)
        {
            if (outputFifoLeftHead == NULL)
            {
                pthread_mutex_unlock(&outputMutex);
                outputThreadCleanUp();
                break;
            }
        }

        //if no items to print wait
        if (outputFifoLeftHead == NULL)
        {
            outputThreadBlocked = true;
            //cout<<"no fifo items, waiting..."<<endl;
            pthread_cond_wait(&outputCond, &outputMutex);
            //cout<<"retrying for fifo items"<<endl;
            pthread_mutex_unlock(&outputMutex);
            continue;
        }
        //else get an item
        else
        {
            //remove FIFO tail
            item = outputFifoRightTail;

            //if fifo has only 1 item left
            if (outputFifoRightTail->prev == NULL)
            {
                //now fifo will be empty
                outputFifoRightTail = outputFifoLeftHead = NULL;
            }
            else
            {
                outputFifoRightTail = outputFifoRightTail->prev;
            }
        }
        pthread_mutex_unlock(&outputMutex);

        //print item

        //calculate timestamp
        if (lastTime == item->stime + item->offset) ts = lastTimestamp;
        else
        {
            getTimestamps(&ts, item->timestamp, item->stime, item->offset);
            lastTime = item->stime + item->offset;
            lastTimestamp = ts;
        }

        //case of all tops
        if ( item->tops >= TOPS )
        {
            //only expiration timestamp
            if (item->offset > 0)
            {
                fprintf(outputFile, "%hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,"
                        "%hd.%hd,%hd.%hd,"
                        "%hd.%hd,%hd.%hd,"
                        "%hd.%hd,%hd.%hd,"
                        "%hd.%hd,%hd.%hd,"
                        "%hd.%hd,%hd.%hd,"
                        "%hd.%hd,%hd.%hd,"
                        "%hd.%hd,%hd.%hd,"
                        "%hd.%hd,%hd.%hd,"
                        "%hd.%hd,%hd.%hd,"
                        "%hd.%hd,%hd.%hd,%li\n",
                        ts.year + 1970, ts.month + 1, ts.day, ts.hour, ts.min, ts.sec,

                        item->routeCells[0].pickX, item->routeCells[0].pickY,
                        item->routeCells[0].dropX, item->routeCells[0].dropY,

                        item->routeCells[1].pickX, item->routeCells[1].pickY,
                        item->routeCells[1].dropX, item->routeCells[1].dropY,

                        item->routeCells[2].pickX, item->routeCells[2].pickY,
                        item->routeCells[2].dropX, item->routeCells[2].dropY,

                        item->routeCells[3].pickX, item->routeCells[3].pickY,
                        item->routeCells[3].dropX, item->routeCells[3].dropY,

                        item->routeCells[4].pickX, item->routeCells[4].pickY,
                        item->routeCells[4].dropX, item->routeCells[4].dropY,

                        item->routeCells[5].pickX, item->routeCells[5].pickY,
                        item->routeCells[5].dropX, item->routeCells[5].dropY,

                        item->routeCells[6].pickX, item->routeCells[6].pickY,
                        item->routeCells[6].dropX, item->routeCells[6].dropY,

                        item->routeCells[7].pickX, item->routeCells[7].pickY,
                        item->routeCells[7].dropX, item->routeCells[7].dropY,

                        item->routeCells[8].pickX, item->routeCells[8].pickY,
                        item->routeCells[8].dropX, item->routeCells[8].dropY,

                        item->routeCells[9].pickX, item->routeCells[9].pickY,
                        item->routeCells[9].dropX, item->routeCells[9].dropY,
                        getFastTimestamp() - item->currentDelay);
            }
            //both pick and dropoff timestamps
            else
            {
                fprintf(outputFile, "%hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,"
                        "%hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,"
                        "%hd.%hd,%hd.%hd,"
                        "%hd.%hd,%hd.%hd,"
                        "%hd.%hd,%hd.%hd,"
                        "%hd.%hd,%hd.%hd,"
                        "%hd.%hd,%hd.%hd,"
                        "%hd.%hd,%hd.%hd,"
                        "%hd.%hd,%hd.%hd,"
                        "%hd.%hd,%hd.%hd,"
                        "%hd.%hd,%hd.%hd,"
                        "%hd.%hd,%hd.%hd,%li\n",
                        ts.year + 1970, ts.month + 1, ts.day, ts.hour, ts.min, ts.sec,
                        item->timestamp.year + 1970, item->timestamp.month + 1, item->timestamp.day,
                        item->timestamp.hour, item->timestamp.min, item->timestamp.sec,

                        item->routeCells[0].pickX, item->routeCells[0].pickY,
                        item->routeCells[0].dropX, item->routeCells[0].dropY,

                        item->routeCells[1].pickX, item->routeCells[1].pickY,
                        item->routeCells[1].dropX, item->routeCells[1].dropY,

                        item->routeCells[2].pickX, item->routeCells[2].pickY,
                        item->routeCells[2].dropX, item->routeCells[2].dropY,

                        item->routeCells[3].pickX, item->routeCells[3].pickY,
                        item->routeCells[3].dropX, item->routeCells[3].dropY,

                        item->routeCells[4].pickX, item->routeCells[4].pickY,
                        item->routeCells[4].dropX, item->routeCells[4].dropY,

                        item->routeCells[5].pickX, item->routeCells[5].pickY,
                        item->routeCells[5].dropX, item->routeCells[5].dropY,

                        item->routeCells[6].pickX, item->routeCells[6].pickY,
                        item->routeCells[6].dropX, item->routeCells[6].dropY,

                        item->routeCells[7].pickX, item->routeCells[7].pickY,
                        item->routeCells[7].dropX, item->routeCells[7].dropY,

                        item->routeCells[8].pickX, item->routeCells[8].pickY,
                        item->routeCells[8].dropX, item->routeCells[8].dropY,

                        item->routeCells[9].pickX, item->routeCells[9].pickY,
                        item->routeCells[9].dropX, item->routeCells[9].dropY,
                        getFastTimestamp() - item->currentDelay);
            }
        }
        //case of less that TOPS tops
        else
        {
            char buf[450];
            short int s;

            if (item->offset > 0)
            {
                s = sprintf(buf, "%02hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,",
                            ts.year + 1970, ts.month + 1, ts.day, ts.hour, ts.min, ts.sec);
            }
            else
            {
                s = sprintf(buf, "%hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,"
                            "%hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,",
                            ts.year + 1970, ts.month + 1, ts.day, ts.hour, ts.min, ts.sec,
                            item->timestamp.year + 1970, item->timestamp.month + 1, item->timestamp.day,
                            item->timestamp.hour, item->timestamp.min, item->timestamp.sec);
            }

            for (short int j = 0; j < item->tops; j++)
            {
                s += sprintf(buf + s, "%hd.%hd,%hd.%hd,",
                             item->routeCells[j].pickX, item->routeCells[j].pickY,
                             item->routeCells[j].dropX, item->routeCells[j].dropY);
            }

            //print nulls
            while ( item->tops < TOPS )
            {
                *(buf + s++) = 'N';
                *(buf + s++) = 'U';
                *(buf + s++) = 'L';
                *(buf + s++) = 'L';
                *(buf + s++) = ',';
                item->tops++;
            }
            buf[s - 1] = '\0';

            fprintf(outputFile, "%s,%li\n", buf, getFastTimestamp() - item->currentDelay);
        }

        #ifdef Q1_TIMING
        Q1_counter_print++;
        #endif

        //push item back to recycler
        pushOutputFifoItemRecycler(item);
    }
    pthread_exit(NULL);
}

void DataStructQ1::pushOutputFifoItem(OutputFifoItem* item)
{
    //cout<<"CALLED"<<endl;
    pthread_mutex_lock(&outputMutex);

    //link to FIFO
    if (outputFifoLeftHead == NULL)
    {
        //empty FIFO
        outputFifoLeftHead = outputFifoRightTail = item;
        item->prev = NULL;
    }
    else
    {
        outputFifoLeftHead->prev = item;
        item->prev = NULL;
        outputFifoLeftHead = item;
    }

    if (outputThreadBlocked)
    {
        //cout<<"singaling"<<endl;
        outputThreadBlocked = false;
        pthread_cond_signal(&outputCond);
    }

    pthread_mutex_unlock(&outputMutex);
}


//OUTPUT FIFO RECYCLER FUNCTIONS
inline void DataStructQ1::popOutputFifoItemRecycler(OutputFifoItem **returnPtr)
{
    pthread_spin_lock(&fifoRecyclerStackLock);

    //case of NOT empty stack
    if (ouputFifoRecyclerStackPtr >= 0)
    {
        *returnPtr = ouputFifoRecyclerStack[ouputFifoRecyclerStackPtr];
        ouputFifoRecyclerStackPtr--;
    }
    //case of empty stack
    else
    {
        *returnPtr = (OutputFifoItem *)malloc(sizeof(OutputFifoItem));
        //cout << "output fifo MALLOCED" <<endl;
        //cout<<++mallocs<<endl;
    }

    pthread_spin_unlock(&fifoRecyclerStackLock);
}


inline void DataStructQ1::pushOutputFifoItemRecycler(OutputFifoItem *item)
{
    pthread_spin_lock(&fifoRecyclerStackLock);
    //case of NOT full stack
    if (ouputFifoRecyclerStackPtr < OUTPUT_FIFO_RECYCLER_STACK_SIZE - 1)
    {
        ouputFifoRecyclerStackPtr++;
        ouputFifoRecyclerStack[ouputFifoRecyclerStackPtr] = item;
    }
    //case of full stack and bucket is not part of initFifoBuckets
    //so free can be called
    //worst case is that the init buckets are unreachable...!
    else if ( item < initOutputFifoItems || item >= initOutputFifoItems + OUTPUT_FIFO_RECYCLER_STACK_SIZE)
    {
        //cout << "output fifo FREED" <<endl;
        free(item);
    }
    pthread_spin_unlock(&fifoRecyclerStackLock);
}
//END OF OUTPUT FIFO RECYCLER FUNCTIONS
#endif
/** END OF PRINT OUTPUT STREAM FUNCTION **/
