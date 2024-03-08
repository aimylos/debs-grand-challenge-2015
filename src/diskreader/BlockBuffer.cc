#include <pthread.h>
#include <stdio.h>

#include "diskreader/BlockBuffer.h"
#include "util/synchronization.h"


int BlockBuffer::indexToWrite = 0; // firstEmpty == firstFull ==> the buffer is EMPTY
int BlockBuffer::indexToRead = NUM_BLOCKS - 1;

pthread_mutex_t BlockBuffer::mutex;
pthread_cond_t BlockBuffer::condAvailableBlock;
pthread_cond_t BlockBuffer::condEmptyBlock;

char BlockBuffer::totalBuffer[NUM_BLOCKS+1][BLOCK_SIZE];
char *BlockBuffer::buffer = totalBuffer[1];
char *BlockBuffer::lastChar[NUM_BLOCKS];

BlockState BlockBuffer::state[NUM_BLOCKS];

bool BlockBuffer::inputCompleted = false;


void BlockBuffer::init()
{
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&condAvailableBlock, NULL);
  pthread_cond_init(&condEmptyBlock, NULL);
}


void BlockBuffer::producedBlock()
{
  pthread_mutex_lock(&mutex);  // enter CR

  // Set empty state to false
  state[indexToWrite].filled = true;

  // Notify the parser that a new block is available
  pthread_cond_signal(&condAvailableBlock);

  // Move write index to next slot
  indexToWrite++;
  indexToWrite %= NUM_BLOCKS;

  // Block if the next slot is still full
  while ( (indexToWrite+1) % NUM_BLOCKS == indexToRead ||
          state[indexToWrite].filled )
  {
#ifdef DEBUG_TIME
    wait_DR_LS++;
    debugTimePause(&time_DR, &started_DR);
#endif
    //printf("---> BlockBuffer is Blocking...\n");
    pthread_cond_wait(&condEmptyBlock, &mutex);
#ifdef DEBUG_TIME
    debugTimeCont(&started_DR);
#endif
  }

  pthread_mutex_unlock(&mutex);  // exit CR
}



void BlockBuffer::waitForAvailableBlock()
{
  pthread_mutex_lock(&mutex);  // enter CR

  // Increment indexToRead, given that I have just processed a block.
  indexToRead++;
  indexToRead %= NUM_BLOCKS;

  // Check if the buffer is full
  while (indexToRead == indexToWrite)
  {
    if (inputCompleted)
    {
      pthread_mutex_unlock(&mutex);  // exit CR
      pthread_exit(NULL);
    }

#ifdef DEBUG_TIME
    wait_LS_DR++;
    debugTimePause(&time_LS, &started_LS);
#endif
    pthread_cond_wait(&condAvailableBlock, &mutex);
#ifdef DEBUG_TIME
    debugTimeCont(&started_LS);
#endif
  }

  pthread_mutex_unlock(&mutex);  // exit CR
}



void BlockBuffer::splitLinesOfBlock(int blockIndex, int numLines)
{
  pthread_mutex_lock(&mutex);

  state[blockIndex].allLinesSplit = true;
  state[blockIndex].linesTotal = numLines;

  pthread_mutex_unlock(&mutex);
}



void BlockBuffer::parsedLineOfBlock(int blockIndex)
{
  pthread_mutex_lock(&mutex);

  if ( (++state[blockIndex].linesParsed == state[blockIndex].linesTotal) &&
       (state[blockIndex].allLinesSplit) )
  {
    state[blockIndex].filled = false;
    state[blockIndex].linesParsed = 0;
    state[blockIndex].allLinesSplit = false;

    // Notify the disk reader that I have processed the previous block
    if ((indexToWrite - indexToRead + NUM_BLOCKS) % NUM_BLOCKS < THRESHOLD_BLOCKS)
      pthread_cond_signal(&condEmptyBlock);
  }

  pthread_mutex_unlock(&mutex);
}
