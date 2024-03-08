#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "data.h"
#include "parameters.h"

#include "query2/dataStructQ2.h"
#include "parser/mktime.h"
#include "lineparser/parser_includes.h"
#include "util/synchronization.h"


/** PRINT OUTPUT STREAM FUNCTION **/
inline void getTimestamps(Timestamp *ts, Timestamp timestamp, time_t stime, short int offset)
{
    *ts = timestamp;
    stime += offset;

    if(offset > 0)
    {
        //case of month increase
        if(stime >= mkTime[timestamp.year*12 + timestamp.month + 1])
        {
            stime = stime - mkTime[timestamp.year*12 + timestamp.month + 1];
            ts->month += 1;
            if(ts->month >= 12) {ts->month=0; ts->year+=1;}
        }
        //case no month increase
        else
        {
            stime = stime - mkTime[timestamp.year*12 + timestamp.month];
        }
    }
    else if(offset < 0)
    {
        //case of month decrease
        if(stime < mkTime[timestamp.year*12 + timestamp.month])
        {
            stime = stime - mkTime[timestamp.year*12 + timestamp.month - 1];
            ts->month -= 1;
            if(ts->month < 0) {ts->month=0; ts->year-=1;}
        }
        //case no month decrease
        else
        {
            stime = stime - mkTime[timestamp.year*12 + timestamp.month];
        }
    }
    else
    {
        //offset is zero
        return;
    }

    ts->day = stime/86400;
    stime %= 86400;
    ts->hour = stime/3600;
    stime %= 3600;
    ts->min = stime/60;
    ts->sec = stime%60;
}


void DataStructQ2::printOutput(Timestamp timestamp, time_t stime, short int offset)
{
#ifdef Q2_DO_OUTPUT_THREAD

    OutputFifoItem *item;
    popOutputFifoItemRecycler(&item);

    item->timestamp = timestamp;
    item->stime = stime;
    item->offset = offset;
    item->currentDelay = currentDelay;
    item->tops = topsSize;
    
    // //get tops
    for(signed char i=0; i<item->tops; i++)
    {
        item->x[i] = topsTable[i]->x;
        item->y[i] = topsTable[i]->y;
        item->emptyTaxis[i] = topsTable[i]->emptyTaxis;
        item->median[i] = topsTable[i]->medianStruct.median;
        item->profit[i] = topsTable[i]->profit;
    }

    //push work to output fifo
    pushOutputFifoItem(item);

#else


    static time_t lastTime=0;
    static Timestamp lastTimestamp;

    //calculate timestamp
    Timestamp ts;
    if(lastTime == stime + offset) ts = lastTimestamp;
    else
    {
        getTimestamps(&ts, timestamp, stime, offset);
        lastTime = stime+offset;
        lastTimestamp = ts;
    }

    //case of all tops
    if(topsSize >= TOPS)
    {
        //only expiration timestamp
        if(offset > 0)
        {
            fprintf(outputFile, "%hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,%li\n",
                ts.year+1970,ts.month+1,ts.day,ts.hour,ts.min,ts.sec,
                
                topsTable[0]->x,topsTable[0]->y,
                topsTable[0]->emptyTaxis, (int)(topsTable[0]->medianStruct.median/100),
                (int)(topsTable[0]->medianStruct.median%100),
                (int)(topsTable[0]->profit/100),
                (int)(topsTable[0]->profit%100),

                topsTable[1]->x,topsTable[1]->y,
                topsTable[1]->emptyTaxis, (int)(topsTable[1]->medianStruct.median/100),
                (int)(topsTable[1]->medianStruct.median%100),
                (int)(topsTable[1]->profit/100),
                (int)(topsTable[1]->profit%100),

                topsTable[2]->x,topsTable[2]->y,
                topsTable[2]->emptyTaxis, (int)(topsTable[2]->medianStruct.median/100),
                (int)(topsTable[2]->medianStruct.median%100),
                (int)(topsTable[2]->profit/100),
                (int)(topsTable[2]->profit%100),

                topsTable[3]->x,topsTable[3]->y,
                topsTable[3]->emptyTaxis, (int)(topsTable[3]->medianStruct.median/100),
                (int)(topsTable[3]->medianStruct.median%100),
                (int)(topsTable[3]->profit/100),
                (int)(topsTable[3]->profit%100),

                topsTable[4]->x,topsTable[4]->y,
                topsTable[4]->emptyTaxis, (int)(topsTable[4]->medianStruct.median/100),
                (int)(topsTable[4]->medianStruct.median%100),
                (int)(topsTable[4]->profit/100),
                (int)(topsTable[4]->profit%100),

                topsTable[5]->x,topsTable[5]->y,
                topsTable[5]->emptyTaxis, (int)(topsTable[5]->medianStruct.median/100),
                (int)(topsTable[5]->medianStruct.median%100),
                (int)(topsTable[5]->profit/100),
                (int)(topsTable[5]->profit%100),

                topsTable[6]->x,topsTable[6]->y,
                topsTable[6]->emptyTaxis, (int)(topsTable[6]->medianStruct.median/100),
                (int)(topsTable[6]->medianStruct.median%100),
                (int)(topsTable[6]->profit/100),
                (int)(topsTable[6]->profit%100),

                topsTable[7]->x,topsTable[7]->y,
                topsTable[7]->emptyTaxis, (int)(topsTable[7]->medianStruct.median/100),
                (int)(topsTable[7]->medianStruct.median%100),
                (int)(topsTable[7]->profit/100),
                (int)(topsTable[7]->profit%100),

                topsTable[8]->x,topsTable[8]->y,
                topsTable[8]->emptyTaxis, (int)(topsTable[8]->medianStruct.median/100),
                (int)(topsTable[8]->medianStruct.median%100),
                (int)(topsTable[8]->profit/100),
                (int)(topsTable[8]->profit%100),

                topsTable[9]->x,topsTable[9]->y,
                topsTable[9]->emptyTaxis, (int)(topsTable[9]->medianStruct.median/100),
                (int)(topsTable[9]->medianStruct.median%100),
                (int)(topsTable[9]->profit/100),
                (int)(topsTable[9]->profit%100),
                getFastTimestamp()-currentDelay);
        }
        //both pick and dropoff timestamps
        else
        {
            fprintf(outputFile, "%hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,"
                                "%hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,%li\n",
                ts.year+1970,ts.month+1,ts.day,ts.hour,ts.min,ts.sec,
                timestamp.year+1970,timestamp.month+1,timestamp.day,
                timestamp.hour,timestamp.min,timestamp.sec,

                topsTable[0]->x,topsTable[0]->y,
                topsTable[0]->emptyTaxis, (int)(topsTable[0]->medianStruct.median/100),
                (int)(topsTable[0]->medianStruct.median%100),
                (int)(topsTable[0]->profit/100),
                (int)(topsTable[0]->profit%100),

                topsTable[1]->x,topsTable[1]->y,
                topsTable[1]->emptyTaxis, (int)(topsTable[1]->medianStruct.median/100),
                (int)(topsTable[1]->medianStruct.median%100),
                (int)(topsTable[1]->profit/100),
                (int)(topsTable[1]->profit%100),

                topsTable[2]->x,topsTable[2]->y,
                topsTable[2]->emptyTaxis, (int)(topsTable[2]->medianStruct.median/100),
                (int)(topsTable[2]->medianStruct.median%100),
                (int)(topsTable[2]->profit/100),
                (int)(topsTable[2]->profit%100),

                topsTable[3]->x,topsTable[3]->y,
                topsTable[3]->emptyTaxis, (int)(topsTable[3]->medianStruct.median/100),
                (int)(topsTable[3]->medianStruct.median%100),
                (int)(topsTable[3]->profit/100),
                (int)(topsTable[3]->profit%100),

                topsTable[4]->x,topsTable[4]->y,
                topsTable[4]->emptyTaxis, (int)(topsTable[4]->medianStruct.median/100),
                (int)(topsTable[4]->medianStruct.median%100),
                (int)(topsTable[4]->profit/100),
                (int)(topsTable[4]->profit%100),

                topsTable[5]->x,topsTable[5]->y,
                topsTable[5]->emptyTaxis, (int)(topsTable[5]->medianStruct.median/100),
                (int)(topsTable[5]->medianStruct.median%100),
                (int)(topsTable[5]->profit/100),
                (int)(topsTable[5]->profit%100),

                topsTable[6]->x,topsTable[6]->y,
                topsTable[6]->emptyTaxis, (int)(topsTable[6]->medianStruct.median/100),
                (int)(topsTable[6]->medianStruct.median%100),
                (int)(topsTable[6]->profit/100),
                (int)(topsTable[6]->profit%100),

                topsTable[7]->x,topsTable[7]->y,
                topsTable[7]->emptyTaxis, (int)(topsTable[7]->medianStruct.median/100),
                (int)(topsTable[7]->medianStruct.median%100),
                (int)(topsTable[7]->profit/100),
                (int)(topsTable[7]->profit%100),

                topsTable[8]->x,topsTable[8]->y,
                topsTable[8]->emptyTaxis, (int)(topsTable[8]->medianStruct.median/100),
                (int)(topsTable[8]->medianStruct.median%100),
                (int)(topsTable[8]->profit/100),
                (int)(topsTable[8]->profit%100),

                topsTable[9]->x,topsTable[9]->y,
                topsTable[9]->emptyTaxis, (int)(topsTable[9]->medianStruct.median/100),
                (int)(topsTable[9]->medianStruct.median%100),
                (int)(topsTable[9]->profit/100),
                (int)(topsTable[9]->profit%100),
                getFastTimestamp()-currentDelay);
        }        
    }
    else
    {
        short int i = 0;
        short int s;
        char buf[450];

        if(offset > 0)
        {
            s = sprintf(buf, "%02hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,", 
                            ts.year+1970,ts.month+1,ts.day,ts.hour,ts.min,ts.sec);
        }
        else
        {
            s = sprintf(buf, "%hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,"
                             "%hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,", 
                            ts.year+1970,ts.month+1,ts.day,ts.hour,ts.min,ts.sec,
                            timestamp.year+1970,timestamp.month+1,timestamp.day,
                            timestamp.hour,timestamp.min,timestamp.sec);
        }

        //print all valid tops cells
        for(; i<topsSize; i++)
        {
            s += sprintf(buf+s, "%hu.%hu,%hd,%d.%.2d,%d.%.2d,", topsTable[i]->x,topsTable[i]->y,
                         topsTable[i]->emptyTaxis, (int)(topsTable[i]->medianStruct.median/100),
                         (int)(topsTable[i]->medianStruct.median%100),
                         (int)(topsTable[i]->profit/100),
                         (int)(topsTable[i]->profit%100));
        }
        //print NULL to the rest
        for(; i<TOPS; i++)
        {
            *(buf + s++) = 'N';
            *(buf + s++) = 'U';
            *(buf + s++) = 'L';
            *(buf + s++) = 'L';
            *(buf + s++) = ',';
        }

        //replace last comma with new line
        buf[s-1] = '\0';

        //print on stdout
        fprintf(outputFile, "%s,%li\n", buf, getFastTimestamp()-currentDelay);
    }

    #ifdef Q2_TIMING
    Q2_counter_print++;
    #endif
#endif
}





#ifdef Q2_DO_OUTPUT_THREAD
void DataStructQ2::outputThreadCleanUp()
{
    //close output file
    fclose(outputFile);

    //printf("Q2 output thread exiting...\n");
}


void *DataStructQ2::outputThread_main(void *)
{
    OutputFifoItem *item;
    time_t lastTime = 0;
    Timestamp lastTimestamp;
    Timestamp ts;

    while(1)
    {
        pthread_mutex_lock(&outputMutex);
         //check if it's time to stop
        if(cleanUpCalled)
        {
            if(outputFifoLeftHead == NULL)
            {
                pthread_mutex_unlock(&outputMutex);
                outputThreadCleanUp();
                break;
            }
        }

        //if no items to print wait
        if(outputFifoLeftHead == NULL)
        {
            outputThreadBlocked=true;
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
            if(outputFifoRightTail->prev == NULL)
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

        //cout<<"printing..."<<endl;

        //calculate timestamp
        if(lastTime == item->stime + item->offset) ts = lastTimestamp;
        else
        {
            getTimestamps(&ts, item->timestamp, item->stime, item->offset);
            lastTime = item->stime + item->offset;
            lastTimestamp = ts;
        }

        //case of all tops
        if( item->tops >= TOPS )
        {
            //only expiration timestamp
            if(item->offset > 0)
            {
                fprintf(outputFile, "%hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,%li\n",
                ts.year+1970,ts.month+1,ts.day,ts.hour,ts.min,ts.sec,
                
                item->x[0],item->y[0],
                item->emptyTaxis[0], (int)(item->median[0]/100),
                (int)(item->median[0]%100),
                (int)(item->profit[0]/100),
                (int)(item->profit[0]%100),

                item->x[1],item->y[1],
                item->emptyTaxis[1], (int)(item->median[1]/100),
                (int)(item->median[1]%100),
                (int)(item->profit[1]/100),
                (int)(item->profit[1]%100),

                item->x[2],item->y[2],
                item->emptyTaxis[2], (int)(item->median[2]/100),
                (int)(item->median[2]%100),
                (int)(item->profit[2]/100),
                (int)(item->profit[2]%100),

                item->x[3],item->y[3],
                item->emptyTaxis[3], (int)(item->median[3]/100),
                (int)(item->median[3]%100),
                (int)(item->profit[3]/100),
                (int)(item->profit[3]%100),

                item->x[4],item->y[4],
                item->emptyTaxis[4], (int)(item->median[4]/100),
                (int)(item->median[4]%100),
                (int)(item->profit[4]/100),
                (int)(item->profit[4]%100),

                item->x[5],item->y[5],
                item->emptyTaxis[5], (int)(item->median[5]/100),
                (int)(item->median[5]%100),
                (int)(item->profit[5]/100),
                (int)(item->profit[5]%100),

                item->x[6],item->y[6],
                item->emptyTaxis[6], (int)(item->median[6]/100),
                (int)(item->median[6]%100),
                (int)(item->profit[6]/100),
                (int)(item->profit[6]%100),

                item->x[7],item->y[7],
                item->emptyTaxis[7], (int)(item->median[7]/100),
                (int)(item->median[7]%100),
                (int)(item->profit[7]/100),
                (int)(item->profit[7]%100),

                item->x[8],item->y[8],
                item->emptyTaxis[8], (int)(item->median[8]/100),
                (int)(item->median[8]%100),
                (int)(item->profit[8]/100),
                (int)(item->profit[8]%100),

                item->x[9],item->y[9],
                item->emptyTaxis[9], (int)(item->median[9]/100),
                (int)(item->median[9]%100),
                (int)(item->profit[9]/100),
                (int)(item->profit[9]%100),

                getFastTimestamp()-item->currentDelay);
            }
            //both pick and dropoff timestamps
            else
            {
                fprintf(outputFile, "%hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,"
                "%hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,"
                "%hu.%hu,%hd,%d.%.2d,%d.%.2d,%li\n",
                ts.year+1970,ts.month+1,ts.day,ts.hour,ts.min,ts.sec,
                item->timestamp.year+1970,item->timestamp.month+1,item->timestamp.day,
                item->timestamp.hour,item->timestamp.min,item->timestamp.sec,
                
                item->x[0],item->y[0],
                item->emptyTaxis[0], (int)(item->median[0]/100),
                (int)(item->median[0]%100),
                (int)(item->profit[0]/100),
                (int)(item->profit[0]%100),

                item->x[1],item->y[1],
                item->emptyTaxis[1], (int)(item->median[1]/100),
                (int)(item->median[1]%100),
                (int)(item->profit[1]/100),
                (int)(item->profit[1]%100),

                item->x[2],item->y[2],
                item->emptyTaxis[2], (int)(item->median[2]/100),
                (int)(item->median[2]%100),
                (int)(item->profit[2]/100),
                (int)(item->profit[2]%100),

                item->x[3],item->y[3],
                item->emptyTaxis[3], (int)(item->median[3]/100),
                (int)(item->median[3]%100),
                (int)(item->profit[3]/100),
                (int)(item->profit[3]%100),

                item->x[4],item->y[4],
                item->emptyTaxis[4], (int)(item->median[4]/100),
                (int)(item->median[4]%100),
                (int)(item->profit[4]/100),
                (int)(item->profit[4]%100),

                item->x[5],item->y[5],
                item->emptyTaxis[5], (int)(item->median[5]/100),
                (int)(item->median[5]%100),
                (int)(item->profit[5]/100),
                (int)(item->profit[5]%100),

                item->x[6],item->y[6],
                item->emptyTaxis[6], (int)(item->median[6]/100),
                (int)(item->median[6]%100),
                (int)(item->profit[6]/100),
                (int)(item->profit[6]%100),

                item->x[7],item->y[7],
                item->emptyTaxis[7], (int)(item->median[7]/100),
                (int)(item->median[7]%100),
                (int)(item->profit[7]/100),
                (int)(item->profit[7]%100),

                item->x[8],item->y[8],
                item->emptyTaxis[8], (int)(item->median[8]/100),
                (int)(item->median[8]%100),
                (int)(item->profit[8]/100),
                (int)(item->profit[8]%100),

                item->x[9],item->y[9],
                item->emptyTaxis[9], (int)(item->median[9]/100),
                (int)(item->median[9]%100),
                (int)(item->profit[9]/100),
                (int)(item->profit[9]%100),

                getFastTimestamp()-item->currentDelay);
            }
        }
        //case of less that TOPS tops
        else
        {
            char buf[450];
            short int s;

            if(item->offset > 0)
            {
                s = sprintf(buf, "%02hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,", 
                                ts.year+1970,ts.month+1,ts.day,ts.hour,ts.min,ts.sec);
            }
            else
            {
                s = sprintf(buf, "%hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,"
                                 "%hd-%02hhd-%02hhd %02hhd:%02hhd:%02hhd,", 
                                ts.year+1970,ts.month+1,ts.day,ts.hour,ts.min,ts.sec,
                                item->timestamp.year+1970,item->timestamp.month+1,item->timestamp.day,
                                item->timestamp.hour,item->timestamp.min,item->timestamp.sec);
            }
        
            for(short int j=0; j<item->tops; j++)
            {
                s += sprintf(buf+s, "%hu.%hu,%hd,%d.%.2d,%d.%.2d,", 
                                item->x[j],item->y[j],
                                item->emptyTaxis[j], (int)(item->median[j]/100),
                                (int)(item->median[j]%100),
                                (int)(item->profit[j]/100),
                                (int)(item->profit[j]%100));
            }

            //print nulls
            while( item->tops<TOPS )
            {
                *(buf + s++) = 'N';
                *(buf + s++) = 'U';
                *(buf + s++) = 'L';
                *(buf + s++) = 'L';
                *(buf + s++) = ',';
                item->tops++;
            }
            buf[s-1] = '\0';

            fprintf(outputFile, "%s,%li\n", buf, getFastTimestamp()-item->currentDelay);
        }

        #ifdef Q2_TIMING
        Q2_counter_print++;
        #endif

        //push item back to recycler
        pushOutputFifoItemRecycler(item);
    }
    pthread_exit(NULL);
}

void DataStructQ2::pushOutputFifoItem(OutputFifoItem* item)
{
    //cout<<"CALLED"<<endl;
    pthread_mutex_lock(&outputMutex);

    //link to FIFO
    if(outputFifoLeftHead == NULL)
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

    if(outputThreadBlocked)
    { 
        //cout<<"singaling"<<endl;
        outputThreadBlocked=false;
        pthread_cond_signal(&outputCond);
    }

    pthread_mutex_unlock(&outputMutex);
}


//OUTPUT FIFO RECYCLER FUNCTIONS
inline void DataStructQ2::popOutputFifoItemRecycler(OutputFifoItem **returnPtr)
{
    pthread_spin_lock(&fifoRecyclerStackLock);

    //case of NOT empty stack
    if(ouputFifoRecyclerStackPtr >= 0)
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


inline void DataStructQ2::pushOutputFifoItemRecycler(OutputFifoItem *item)
{
    pthread_spin_lock(&fifoRecyclerStackLock);
    //case of NOT full stack
    if(ouputFifoRecyclerStackPtr < OUTPUT_FIFO_RECYCLER_STACK_SIZE-1)
    {
        ouputFifoRecyclerStackPtr++;
        ouputFifoRecyclerStack[ouputFifoRecyclerStackPtr] = item;
    }
    //case of full stack and bucket is not part of initFifoBuckets
    //so free can be called
    //worst case is that the init buckets are unreachable...!
    else if( item<initOutputFifoItems || item>=initOutputFifoItems+OUTPUT_FIFO_RECYCLER_STACK_SIZE)
    {
        //cout << "output fifo freed" <<endl;
        free(item);
    }
    pthread_spin_unlock(&fifoRecyclerStackLock);
}
//END OF OUTPUT FIFO RECYCLER FUNCTIONS
#endif



/** END OF PRINT OUTPUT STREAM FUNCTION **/
