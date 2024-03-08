#include <iostream>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
using namespace std;

#include "parameters.h"
#include "data.h"

#ifdef Q2_SUBMIT_A
#include "../src/query2/dataStructQ2.h"
#else
#include "query2/dataStructQ2.h"
#endif


/** MEDIAN INSERT FUNCTIONS **/
void DataStructQ2::insertMedian(Median *median, MedianKey key, MedianBucket **newBucket)
{
    popMedianBucketRecycler(newBucket);
    (*newBucket)->key = key;

    //check if median is empty
    if(median->leftHeapSize == 0  &&  median->rightHeapSize == 0)
    {
        //insert new median key into left heap
        insertLeftHeap(median, *newBucket);
        median->leftHeapSize = 1;
        median->heaverHeap = -1;
        median->median = key;
        return;
    }

    //left heap is equal or heaver than right heap
    if(median->heaverHeap <= 0)
    {
        //new key can be inserted into right heap because it is greater or equal to left heap root
        if(median->leftHeap[0]->key <= key)
        {
            insertRightHeap(median, *newBucket);

            //if heaps were equal before insertion to right heap
            if(median->heaverHeap == 0)
            {
                //right heap will be now heavier
                median->heaverHeap = 1;
                //return right heap root
                median->median = median->rightHeap[0]->key;
            }
            //else left heap was hevier before insertion to right heap
            else
            {
                //heaps will be equal now
                median->heaverHeap = 0;
                //return average
                median->median = (median->leftHeap[0]->key + median->rightHeap[0]->key)/2;
            }
        }
        //else if new key has to go into left heap and heaps are equal insert new key into left
        else if(median->heaverHeap == 0)
        {
            insertLeftHeap(median, *newBucket);
            //left heap is now heavier
            median->heaverHeap = -1;
            //return left heap root
            median->median = median->leftHeap[0]->key;
        }
        //else new key has to be inserted into left heap but left heap is too heavy
        else
        {
            //make left heap lighter by inserting it's root into right heap
            insertRightHeap(median, median->leftHeap[0]);

            //replace left heap root with new key
            replaceLeftHeapRoot(median, *newBucket);

            //since left heap was heavier now they will be equal
            median->heaverHeap = 0;
            //return average
            median->median = (median->leftHeap[0]->key + median->rightHeap[0]->key)/2;
        }
    }
    //else right heap is heaver than left heap
    else
    {
        //new key can be inserted into left heap because it is less or equal to right heap root
        if(median->rightHeap[0]->key >= key)
        {
            insertLeftHeap(median, *newBucket);
        }
        //else new key has to be inserted into right reap but right heap is too heavy
        else
        {
            //make right heap lighter by inserting it's root into left heap
            insertLeftHeap(median, median->rightHeap[0]);

            //replace right heap root with new key
            replaceRightHeapRoot(median, *newBucket);
        }

        //since right heap was heavier now they will be equal
        median->heaverHeap = 0;
        //return average
        median->median = (median->leftHeap[0]->key + median->rightHeap[0]->key)/2;
    }

    return;
}


inline void DataStructQ2::insertLeftHeap(Median* median, MedianBucket *bucket)
{
    //means bucket is part of left heap
    bucket->lrHeap = -1;

    //check if heap is not full
    if(median->leftHeapSize < MAX_HEAP)
    {
        //assign index to bucket
        bucket->index = median->leftHeapSize;

        //insert bucket to last place
        median->leftHeap[median->leftHeapSize] = bucket;

        //increase left heap size
        median->leftHeapSize++;

        //heapify up
        //shared code in the end
    }
    //else the heap is full
    else
    {
        //mark last item as if it was part of no heap
        median->leftHeap[MAX_HEAP-1]->lrHeap = 0;

        //assign index to bucket
        bucket->index = MAX_HEAP-1;

        //replace last item with bucket
        median->leftHeap[MAX_HEAP-1] = bucket;

        //heapify up
        //shared code in the end
    }

    /* HEAPIFY UP FOR MAX-HEAP*/
    HeapSize_t j,i = bucket->index;
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
        //if parent is less than bucket_i swap
        if(median->leftHeap[j]->key < bucket->key)
        {
            //swap buckets
            median->leftHeap[i] = median->leftHeap[j];
            median->leftHeap[j] = bucket;
            //swap indexes, this is for window time fifo
            median->leftHeap[i]->index = i;
            median->leftHeap[j]->index = j;
            i = j;
        }
        else
        {
            break;
        }
    }
    /* END OF HEAPIFY UP FOR MAX-HEAP */
}


inline void DataStructQ2::insertRightHeap(Median* median, MedianBucket *bucket)
{
    //means bucket is part of right heap
    bucket->lrHeap = 1;

    //check if heap is not full
    if(median->rightHeapSize < MAX_HEAP)
    {
        //assign index to bucket
        bucket->index = median->rightHeapSize;

        //insert bucket to last place
        median->rightHeap[median->rightHeapSize] = bucket;

        //increase heap size
        median->rightHeapSize++;

        //heapify up
        //shared code in the end
    }
    //else the heap is full
    else
    {
        //mark last item as if it was part of no heap
        median->rightHeap[MAX_HEAP-1]->lrHeap = 0;

        //assign index to bucket
        bucket->index = MAX_HEAP-1;

        //replace last item with bucket
        median->rightHeap[MAX_HEAP-1] = bucket;

        //heapify up
        //shared code in the end
    }

    /* HEAPIFY UP FOR MIN-HEAP */
    HeapSize_t j,i = bucket->index;
    //given an index i:
    //parent at: floor((i-1)/2)
    //chilren at: 2i+1, 2i+2
    //heapify up: if parent is GREATER than given index swap
    //if swap is not needed end of heapify up

    //while this is not root
    while(i>0)
    {
        //parnet index
        j = (i-1)/2;
        //if parent is more than bucket_i swap
        if(median->rightHeap[j]->key > bucket->key)
        {
            //swap buckets
            median->rightHeap[i] = median->rightHeap[j];
            median->rightHeap[j] = bucket;
            //swap indexes, this is for window time fifo
            median->rightHeap[i]->index = i;
            median->rightHeap[j]->index = j;
            i = j;
        }
        else
        {
            break;
        }
    }
    /* END OF HEAPIFY UP FOR MIN-HEAP */
}


inline void DataStructQ2::replaceLeftHeapRoot(Median* median, MedianBucket *bucket)
{
    //means bucket is part of left heap
    bucket->lrHeap = -1;

    //replace root with bucket
    median->leftHeap[0] = bucket;

    //update bucket index
    bucket->index = 0;

    /* HEAPIFY DOWN FOR MAX HEAP */
    HeapSize_t j, i = 0;
    //given an index i:
    //parent at: floor((i-1)/2)
    //chilren at: 2i+1, 2i+2
    //heapify down for max heap: if the greater child of i
    //is greater than parent then swap
    //if swap is not needed end of heapify down

    //while i has at least one child
    while(2*i+1 < median->leftHeapSize)
    {
        //get index to greater child but verify if both children exist
        if(2*i+2 < median->leftHeapSize)
        {
            //both children exist
            j = (median->leftHeap[2*i+1]->key > median->leftHeap[2*i+2]->key)?(2*i+1):(2*i+2);
        }
        else
        {
            //only one child exist
            j = 2*i+1;
        }

        //if child is greater than parent swap
        if(median->leftHeap[j]->key > bucket->key)
        {
            //swap buckets
            median->leftHeap[i] = median->leftHeap[j];
            median->leftHeap[j] = bucket;
            //swap indexes, this is for window time fifo
            median->leftHeap[i]->index = i;
            median->leftHeap[j]->index = j;
            i = j;
        }
        else
        {
            break;
        }
    }
    /* END OF HEAPIFY DOWN FOR MAX-HEAP */
}


inline void DataStructQ2::replaceRightHeapRoot(Median* median, MedianBucket *bucket)
{
    //means bucket is part of right heap
    bucket->lrHeap = 1;

    //replace root with bucket
    median->rightHeap[0] = bucket;

    //update bucket index
    bucket->index = 0;

    /* HEAPIFY DOWN FOR MIN HEAP */
    HeapSize_t j, i = 0;
    //given an index i:
    //parent at: floor((i-1)/2)
    //chilren at: 2i+1, 2i+2
    //heapify down for min heap: if the smaller child of i
    //is less than parent then swap
    //if swap is not needed end of heapify down

    //while i has at least one child
    while(2*i+1 < median->rightHeapSize)
    {
        //get index to smaller child but verify if both children exist
        if(2*i+2 < median->rightHeapSize)
        {
            //both children exist
            j = (median->rightHeap[2*i+1]->key < median->rightHeap[2*i+2]->key)?(2*i+1):(2*i+2);
        }
        else
        {
            //only one child exist
            j = 2*i+1;
        }

        //if child is less than parent swap
        if(median->rightHeap[j]->key < bucket->key)
        {
            //swap buckets
            median->rightHeap[i] = median->rightHeap[j];
            median->rightHeap[j] = bucket;
            //swap indexes, this is for window time fifo
            median->rightHeap[i]->index = i;
            median->rightHeap[j]->index = j;
            i = j;
        }
        else
        {
            break;
        }
    }
    /* END OF HEAPIFY DOWN FOR MIN-HEAP */
}
/** END OF MEDIAN INSERT FUNCTIONS **/



/** MEDIAN DELETE FUNCTIONS **/
void DataStructQ2::deleteMedian(Median* median, MedianBucket* medianBucket)
{
    //find out which heap medianBucket belongs to
    //if it belongs to the left heap
    if(medianBucket->lrHeap < 0)
    {
        //if left heap is heavier or heaps are equal
        if(median->heaverHeap <= 0)
        {
            //delete medianBucket from left heap
            deleteLeftHeap(median, medianBucket);
            //increase heaverHeap towards the right
            median->heaverHeap++;
        }
        //else right heap is heavier and left heap it's about to get even ligher
        else
        {
            //get the heap's root
            MedianBucket *tempRoot = median->rightHeap[0];
            //delete rightHeap's root
            deleteRightHeap(median, tempRoot);
            //replace medianBucket with rightHeap's root
            replaceLeftHeapWithGreater(median, medianBucket, tempRoot);
            //heaps are now equal
            median->heaverHeap = 0;
        }
    }
    //else if it belongs to the right heap
    else if(medianBucket->lrHeap > 0)
    {
        //if right heap is heavier or heaps are equal
        if(median->heaverHeap >= 0)
        {
            //delete medianBucket from right heap
            deleteRightHeap(median, medianBucket);
            //decrease heaverHeap towards the left
            median->heaverHeap--;
        }
        //else left heap is heavier and right heap it's about to get even ligher
        else
        {
            //get the heap's root
            MedianBucket *tempRoot = median->leftHeap[0];
            //delete leftHeap's root
            deleteLeftHeap(median, tempRoot);
            //replace medianBucket with rightHeap's root
            replaceRightHeapWithSmaller(median, medianBucket, tempRoot);
            //heaps are now equal
            median->heaverHeap = 0;
        }
    }
    //else medianBucket does not belong to a heap, lrheap==0
    else
    {
        //release medianBucket
        pushMedianBucketRecycler(medianBucket);

        //no need to update median
        return;
    }

    //release medianBucket
    pushMedianBucketRecycler(medianBucket);

    //update median
    //if heaps are equal
    if(median->heaverHeap == 0)
    {
        //check is heaps are empty
        if(median->leftHeapSize == 0)
        {
            //set median to 0
            median->median = 0;
        }
        else
        {
            median->median = (median->leftHeap[0]->key + median->rightHeap[0]->key)/2;
        }
    }
    //else if left heap is heavier
    else if(median->heaverHeap < 0)
    {
        median->median = median->leftHeap[0]->key;
    }
    //else right heap is heavier
    else
    {
        median->median = median->rightHeap[0]->key;
    }
}


inline void DataStructQ2::deleteLeftHeap(Median *median, MedianBucket *bucket)
{
    //check if heap is empty
    if(median->leftHeapSize == 0) return;

    //decrease left heap size
    median->leftHeapSize--;

    //if about to delete last leaf just return
    if(median->leftHeapSize == bucket->index) return;

    //replace bucket with last leaf
    median->leftHeap[bucket->index] = median->leftHeap[median->leftHeapSize];
    //update previous last leaf index
    median->leftHeap[bucket->index]->index = bucket->index;

    bucket = median->leftHeap[bucket->index];

    /* HEAPIFY DOWN FOR MAX HEAP */
    HeapSize_t j, i = bucket->index;
    //given an index i:
    //parent at: floor((i-1)/2)
    //chilren at: 2i+1, 2i+2
    //heapify down for max heap: if the greater child of i
    //is greater than parent then swap
    //if swap is not needed end of heapify down

    //while i has at least one child
    while(2*i+1 < median->leftHeapSize)
    {
        //get index to greater child but verify if both children exist
        if(2*i+2 < median->leftHeapSize)
        {
            //both children exist
            j = (median->leftHeap[2*i+1]->key > median->leftHeap[2*i+2]->key)?(2*i+1):(2*i+2);
        }
        else
        {
            //only one child exist
            j = 2*i+1;
        }

        //if child is greater than parent swap
        if(median->leftHeap[j]->key > bucket->key)
        {
            //swap buckets
            median->leftHeap[i] = median->leftHeap[j];
            median->leftHeap[j] = bucket;
            //swap indexes, this is for window time fifo
            median->leftHeap[i]->index = i;
            median->leftHeap[j]->index = j;
            i = j;
        }
        else
        {
            break;
        }
    }
    /* END OF HEAPIFY DOWN FOR MAX-HEAP */
}


inline void DataStructQ2::deleteRightHeap(Median *median, MedianBucket *bucket)
{
    //check if heap is empty
    if(median->rightHeapSize == 0) return;

    //decrease right heap size
    median->rightHeapSize--;

    //if about to delete last leaf just return
    if(median->rightHeapSize == bucket->index) return;

    //replace bucket with last leaf
    median->rightHeap[bucket->index] = median->rightHeap[median->rightHeapSize];
    //update previous last leaf index
    median->rightHeap[bucket->index]->index = bucket->index;

    bucket = median->rightHeap[bucket->index];

    /* HEAPIFY DOWN FOR MIN HEAP */
    HeapSize_t j, i = bucket->index;
    //given an index i:
    //parent at: floor((i-1)/2)
    //chilren at: 2i+1, 2i+2
    //heapify down for min heap: if the smaller child of i
    //is less than parent then swap
    //if swap is not needed end of heapify down

    //while i has at least one child
    while(2*i+1 < median->rightHeapSize)
    {
        //get index to smaller child but verify if both children exist
        if(2*i+2 < median->rightHeapSize)
        {
            //both children exist
            j = (median->rightHeap[2*i+1]->key < median->rightHeap[2*i+2]->key)?(2*i+1):(2*i+2);
        }
        else
        {
            //only one child exist
            j = 2*i+1;
        }

        //if child is less than parent swap
        if(median->rightHeap[j]->key < bucket->key)
        {
            //swap buckets
            median->rightHeap[i] = median->rightHeap[j];
            median->rightHeap[j] = bucket;
            //swap indexes, this is for window time fifo
            median->rightHeap[i]->index = i;
            median->rightHeap[j]->index = j;
            i = j;
        }
        else
        {
            break;
        }
    }
    /* END OF HEAPIFY DOWN FOR MIN-HEAP */
}


inline void DataStructQ2::replaceLeftHeapWithGreater(Median *median, MedianBucket *oldBucket, MedianBucket *newBucket)
{
    //check if heap is empty
    if(median->leftHeapSize == 0) return;

    //replace oldBucket with newBucket
    median->leftHeap[oldBucket->index] = newBucket;
    //update newBucket index
    newBucket->index = oldBucket->index;
    //mark newBucket as part of left heap
    newBucket->lrHeap = -1;

    /* HEAPIFY UP FOR MAX-HEAP*/
    HeapSize_t j,i = newBucket->index;
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
        //if parent is less than bucket_i swap
        if(median->leftHeap[j]->key < newBucket->key)
        {
            //swap buckets
            median->leftHeap[i] = median->leftHeap[j];
            median->leftHeap[j] = newBucket;
            //swap indexes, this is for window time fifo
            median->leftHeap[i]->index = i;
            median->leftHeap[j]->index = j;
            i = j;
        }
        else
        {
            break;
        }
    }
    /* END OF HEAPIFY UP FOR MAX-HEAP */
}


inline void DataStructQ2::replaceRightHeapWithSmaller(Median *median, MedianBucket *oldBucket, MedianBucket *newBucket)
{
    //check if heap is empty
    if(median->rightHeapSize == 0) return;

    //replace oldBucket with newBucket
    median->rightHeap[oldBucket->index] = newBucket;
    //update newBucket index
    newBucket->index = oldBucket->index;
    //mark newBucket as part of right heap
    newBucket->lrHeap = 1;

    /* HEAPIFY UP FOR MIN-HEAP */
    HeapSize_t j,i = newBucket->index;
    //given an index i:
    //parent at: floor((i-1)/2)
    //chilren at: 2i+1, 2i+2
    //heapify up: if parent is GREATER than given index swap
    //if swap is not needed end of heapify up

    //while this is not root
    while(i>0)
    {
        //parnet index
        j = (i-1)/2;
        //if parent is more than bucket_i swap
        if(median->rightHeap[j]->key > newBucket->key)
        {
            //swap buckets
            median->rightHeap[i] = median->rightHeap[j];
            median->rightHeap[j] = newBucket;
            //swap indexes, this is for window time fifo
            median->rightHeap[i]->index = i;
            median->rightHeap[j]->index = j;
            i = j;
        }
        else
        {
            break;
        }
    }
    /* END OF HEAPIFY UP FOR MIN-HEAP */
}
/** END OF MEDIAN DELETE FUNCTIONS **/


/** MEDIANBUCKET RECYCLER **/
inline void DataStructQ2::popMedianBucketRecycler(MedianBucket **returnPtr)
{
    //static int mallocs=0;
    //case of NOT empty stack
    if(medianBucketStackPtr >= 0)
    {
        *returnPtr = medianBucketStack[medianBucketStackPtr];
        medianBucketStackPtr--;
    }
    //case of empty stack
    else
    {
        *returnPtr = (MedianBucket *)malloc(sizeof(MedianBucket));
        //cout<<++mallocs<<endl;
    }
}

inline void DataStructQ2::pushMedianBucketRecycler(MedianBucket *bucket)
{
    //case of NOT full stack
    if(medianBucketStackPtr < MEDIAN_STACK_SIZE-1)
    {
        medianBucketStackPtr++;
        medianBucketStack[medianBucketStackPtr] = bucket;
    }
    //case of full stack and bucket is not part of initFifoBuckets
    //so free can be called
    else if( bucket<initMedianBuckets || bucket>=initMedianBuckets+MEDIAN_STACK_SIZE_INIT)
    {
        free(bucket);
    }
}
/** END OF MEDIANBUCKET RECYCLER **/
