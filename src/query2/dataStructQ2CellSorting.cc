#include <iostream>
#include <stdio.h>
#include <string.h>
#include <time.h>
using namespace std;

#include "parameters.h"
#include "data.h"

#ifdef Q2_SUBMIT_A
#include "../src/query2/dataStructQ2.h"
#else
#include "query2/dataStructQ2.h"
#endif


void DataStructQ2::updateCellSorting(CellBucket *bucket)
{
    //previous place used for tops changed
    signed char partOf = bucket->partOf;
    TableSize_t index = bucket->index;

    //keep previous profitabiliy
    ProfitKey previousProfit = bucket->profit;

    //calculate new profitability
    bucket->profit = (bucket->emptyTaxis <= 0)?(-1):(bucket->medianStruct.median / bucket->emptyTaxis);
    
    //case of invalid new profitability
    //or
    //case of 0 profitabiliy and heap root is greater than 0
    if( (bucket->profit < 0) || (bucket->profit == 0 && heapSize > 0 && mainHeap[0]->profit > 0) )
    {
        //case of cell being part of some structure
        if(bucket->partOf != 0)
        {
            //delete from sorting structure
            deleteCellFromSorting(bucket);
        }
        //case of cell NOT being part of any structure
        else
        {
            return;
        }
    }
    //case of valid new profitability
    else
    {
        //case of cell NOT being part of any sturcture
        if(bucket->partOf == 0)
        {
            //insert into sorting structure
            insertCellIntoSorting(bucket);
            //cout<<"INSERT CELL"<<endl;
        }
        //case of increase
        else if(bucket->profit >= previousProfit)
        {
            //update increased key
            updateCellIncreased(bucket);
            //cout<<"INCREASED CELL"<<endl;
        }
        else
        //case of decrease
        {
            //updated decreased key
            updateCellDecreased(bucket);
            //cout<<"DECREASED CELL"<<endl;
        }

    }
    
    //update topsChanged
    if( (partOf==1 && bucket->partOf==1 && bucket->index!=index)
            || (partOf==1 && bucket->partOf!=1) || (partOf!=1 && bucket->partOf==1) )
    {
        topsChanged = true;
    }
}


/** CELLBUCKET SORTING STRUCTURE **/
inline void DataStructQ2::insertCellIntoSorting(CellBucket *bucket)
{
    //case of empty tops
    //is not needed, the next case takes care of it
    //code for this case exist in older versions
    //case of less than TOPS tops
    if(topsSize < TOPS)
    {
        //place bucket to first empty place
        topsTable[topsSize] = bucket;
        //give bucket the right index and partOf
        bucket->partOf = 1;
        bucket->index = topsSize;

        //increase table size
        topsSize++;

        //re-order tops right
        updateTopsTowardsLeft(bucket);
    }
    //case of more than TOPS and insert to main heap
    else if(topsTable[TOPS-1]->profit > bucket->profit)
    {
        //insert bucket into main heap
        insertIntoMainHeap(bucket);
    }
    //case of bucket going into tops but tops is full
    else
    {
        //insert last of tops into main heap
        insertIntoMainHeap(topsTable[TOPS-1]);

        //replace last top with bucket
        replaceConstTopsLast(bucket);
    }
}


inline void DataStructQ2::updateCellIncreased(CellBucket *bucket)
{
    //case of being part of tops
    if(bucket->partOf == 1)
    {
        updateTopsTowardsLeft(bucket);
    }
    //case of being part of main heap
    else if(bucket->partOf == 2)
    {
        //check if bucket needs to go into tops
        if(topsTable[TOPS-1]->profit <= bucket->profit)
        {
            //replace bucket with tops last and heapify up
            mainHeap[bucket->index] = topsTable[TOPS-1];
            mainHeap[bucket->index]->partOf = 2;
            mainHeap[bucket->index]->index = bucket->index;
            heapfyUpMainHeap(mainHeap[bucket->index]);

            //place bucket in last tops' and update tops order
            replaceConstTopsLast(bucket);
        }
        //else just heapify up
        else
        {
            heapfyUpMainHeap(bucket);
        }
    }
    //case of being part of nothing
    //just return
}


inline void DataStructQ2::deleteCellFromSorting(CellBucket *bucket)
{
    //case of being part of tops
    if(bucket->partOf == 1)
    {
        //get bucket's index
        signed char i = bucket->index;
        //mark as part of nothing
        bucket->partOf = 0;

        //shift everything one place to the left
        for(; i<topsSize-1; i++)
        {
            topsTable[i] = topsTable[i+1];
            topsTable[i]->index = i;
        }

        //if mainHeap is empty decrease topsSize
        if(heapSize == 0)
        {
            topsSize--;
        }
        //else take root from mainHeap and put it in last place of tops
        else
        {
            //get the main heap's root
            topsTable[TOPS-1] = mainHeap[0];

            //delete main heap's root
            deleteFromMainHeap(mainHeap[0]);

            //mark previous heap root as part of tops
            topsTable[TOPS-1]->partOf = 1;
            topsTable[TOPS-1]->index = TOPS-1;
        }
    }
    //case of being part of main heap
    else if(bucket->partOf == 2)
    {
        //delete from heap
        deleteFromMainHeap(bucket);
    }
    //case of being part of nothing
}


inline void DataStructQ2::updateCellDecreased(CellBucket *bucket)
{
    //case of being part of tops
    if(bucket->partOf == 1)
    {
        //re-order tops
        updateTopsTowardsRight(bucket);

        //check if bucket is the last tops and main heap ain't empty
        //and if yes check if main heap's root is greater
        if(heapSize>0 && bucket->index==TOPS-1)
        {
            if(bucket->profit < mainHeap[0]->profit)
            {
                //get the main heap's root
                topsTable[TOPS-1] = mainHeap[0];

                //replace main heap's root with bucket
                replaceRootMainHeap(bucket);

                //mark previous heap root as part of tops
                topsTable[TOPS-1]->partOf = 1;
                topsTable[TOPS-1]->index = TOPS-1;
            }
        }
    }
    //case of being part of main heap
    else if(bucket->partOf == 2)
    {
        //heapify down
        heapfyDownMainHeap(bucket);
    }
}
/** END OF CELLBUCKET SORTING STRUCTURE **/


/** TOPS FUNCTIONS **/
inline void DataStructQ2::updateTopsTowardsLeft(CellBucket *bucket)
{
    //bucket's left
    //signed char N = bucket->index;
    signed char i = bucket->index-1;

    //move cells to the right
    while(i>=0)
    {
        if(topsTable[i]->profit > bucket->profit) break;
        //move to right
        else
        {
            topsTable[i+1] = topsTable[i];
            topsTable[i+1]->index = i+1;
            i--;
        }
    }

    //place bucket to right place
    topsTable[i+1] = bucket;
    bucket->index = i+1;
}


inline void DataStructQ2::updateTopsTowardsRight(CellBucket *bucket)
{
    signed char i = bucket->index;
    //swap towards the right
    while(i<topsSize-1)
    {
        if(bucket->profit < topsTable[i+1]->profit)
        {
            //bring the right one place left
            topsTable[i] = topsTable[i+1];
            topsTable[i]->index = i;
            i++;
        }
        else
        {
            break;
        }
    }

    //place bucket to the right place
    topsTable[i] = bucket;
    bucket->index = i;
}


inline void DataStructQ2::replaceConstTopsLast(CellBucket *bucket)
{
    //give bucket partOf
    bucket->partOf = 1;
    
    //bucket's left
    signed char i = TOPS-2;

    //move cells to the right
    while(i>=0)
    {
        if(topsTable[i]->profit > bucket->profit) break;
        //move to right
        else
        {
            topsTable[i+1] = topsTable[i];
            topsTable[i+1]->index = i+1;
            i--;
        }
    }

    //place bucket to right place
    topsTable[i+1] = bucket;
    bucket->index = i+1;
}
/** END OF TOPS FUNCTIONS **/

/** MAIN MAX-HEAP FUNCTIONS **/
inline void DataStructQ2::insertIntoMainHeap(CellBucket* bucket)
{
    //don't insert if heap is full, sould NEVER happen
    //and this check is redundant
    if(heapSize>=CELL_TABLE_SIZE) return;

    //mark as part of heap
    bucket->partOf = 2;

    //assign index to bucket
    bucket->index = heapSize;

    //insert bucket to last place
    mainHeap[heapSize] = bucket;

    //increase left heap size
    heapSize++;

    //heapify up
    heapfyUpMainHeap(bucket);
}


inline void DataStructQ2::deleteFromMainHeap(CellBucket* bucket)
{
    //check if heap is empty
    if(heapSize <= 0) return;

    //mark as part of nothing
    bucket->partOf = 0;

    //decrease heap size
    heapSize--;

    //if about to delete last leaf just return
    if(heapSize == bucket->index) return;

    //replace bucket with last leaf
    mainHeap[bucket->index] = mainHeap[heapSize];

    //update previous last leaf index
    mainHeap[bucket->index]->index = bucket->index;

    //heapify down
    heapfyDownMainHeap(mainHeap[bucket->index]);
}


inline void DataStructQ2::replaceRootMainHeap(CellBucket *bucket)
{
    if(heapSize <= 0) return;

    //mark old root as part of nothing
    mainHeap[0]->partOf = 0;

    //replace root
    mainHeap[0] = bucket;

    //mark bucket as part of main heap
    bucket->partOf = 2;

    //give bucket index
    bucket->index = 0;

    //heapfy down
    heapfyDownMainHeap(bucket);
}


void DataStructQ2::heapfyUpMainHeap(CellBucket* bucket)
{
    /* HEAPIFY UP FOR MAX-HEAP*/
    TableSize_t j,i = bucket->index;
    //given an index i:
    //parent at: floor((i-1)/2)
    //chilren at: 2i+1, 2i+2
    //heapify up: if parent is LESS than given index swap
    //if swap is not needed end of heapify up
    //while this is not root
    while(i>0)
    {
        //parent
        j = (i-1)/2;
        //if parent is less or equal(freshest first) than bucket_i swap
        if(mainHeap[j]->profit <= bucket->profit)
        {
            //swap buckets
            mainHeap[i] = mainHeap[j];
            mainHeap[j] = bucket;
            //swap indexes, this is for delete
            mainHeap[i]->index = i;
            mainHeap[j]->index = j;
            i = j;
        }
        else
        {
            break;
        }
    }
    /* END OF HEAPIFY UP FOR MAX-HEAP */
}


void DataStructQ2::heapfyDownMainHeap(CellBucket* bucket)
{
    /* HEAPIFY DOWN FOR MAX HEAP */
    TableSize_t j, i = bucket->index;
    //given an index i:
    //parent at: floor((i-1)/2)
    //chilren at: 2i+1, 2i+2
    //heapify down for max heap: if the greater child of i
    //is greater than parent then swap
    //if swap is not needed end of heapify down

    //while i has at least one child
    while(2*i+1 < heapSize)
    {
        //get index to greater child but verify if both children exist
        if(2*i+2 < heapSize)
        {
            //both children exist
            j = (mainHeap[2*i+1]->profit > mainHeap[2*i+2]->profit)?(2*i+1):(2*i+2);
        }
        else
        {
            //only one child exist
            j = 2*i+1;
        }

        //if child is greater than parent swap
        if(mainHeap[j]->profit > bucket->profit)
        {
            //swap buckets
            mainHeap[i] = mainHeap[j];
            mainHeap[j] = bucket;
            //swap indexes, this is for window time fifo
            mainHeap[i]->index = i;
            mainHeap[j]->index = j;
            i = j;
        }
        else
        {
            break;
        }
    }
    /* END OF HEAPIFY DOWN FOR MAX-HEAP */
}
/** END OF MAIN MAX-HEAP FUNCTIONS **/