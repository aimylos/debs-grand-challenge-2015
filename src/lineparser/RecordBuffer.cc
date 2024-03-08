#include <stdio.h>

#include "lineparser/RecordBuffer.h"
#include "diskreader/BlockBuffer.h"
#include "util/synchronization.h"
#include "parameters.h"


int RecordBuffer::indexToWrite = NUM_RECORDS - 1;
int RecordBuffer::indexToReadQ1 = NUM_RECORDS - 1;
int RecordBuffer::indexToReadQ2 = NUM_RECORDS - 1;

pthread_mutex_t RecordBuffer::mutex;
pthread_cond_t RecordBuffer::condFreeSlot;
pthread_cond_t RecordBuffer::condAvailableRecordQ1;
pthread_cond_t RecordBuffer::condAvailableRecordQ2;

Record RecordBuffer::records[NUM_RECORDS];



void RecordBuffer::init()
{
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&condFreeSlot, NULL);
  pthread_cond_init(&condAvailableRecordQ1, NULL);
  pthread_cond_init(&condAvailableRecordQ2, NULL);
}



Record *RecordBuffer::getRecordLS()
{
  pthread_mutex_lock(&mutex);  // enter CR

  // First, increment indexToWrite
  indexToWrite++;
  indexToWrite %= NUM_RECORDS;

  // And then, check if the buffer is full
  while ( (indexToWrite+1) % NUM_RECORDS == indexToReadQ2 ||
          (indexToWrite+1) % NUM_RECORDS == indexToReadQ1 ||
          records[indexToWrite].state != 0 )
  {
#ifdef DEBUG_TIME
    wait_LS_QR++;
    debugTimePause(&time_LS, &started_LS);
#endif
    pthread_cond_wait(&condFreeSlot, &mutex);
#ifdef DEBUG_TIME
    debugTimeCont(&started_LS);
#endif
  }


  Record *record = &records[indexToWrite];
  record->state = RECORD_READY_FOR_LP;

  pthread_mutex_unlock(&mutex);  // exit CR

  return record;
}



void RecordBuffer::submitRecord(Record *record)
{
  pthread_mutex_lock(&mutex);  // enter CR

  // Prepare 'state' for precisely two consumers: Q1, Q2
  record->state ^= RECORD_READY_FOR_Q1 | RECORD_READY_FOR_Q2 | RECORD_READY_FOR_LP;

  // Notify the query threads that a new record has been produced
  pthread_cond_signal(&condAvailableRecordQ1);
  pthread_cond_signal(&condAvailableRecordQ2);

  pthread_mutex_unlock(&mutex);  // exit CR
}



Record *RecordBuffer::getRecordQ1()
{
  pthread_mutex_lock(&mutex);

  if ( (records[indexToReadQ1].state &= ~RECORD_READY_FOR_Q1) == 0 &&
       (indexToWrite - indexToReadQ2 + NUM_RECORDS) % NUM_RECORDS < THRESHOLD_RECORDS)
    pthread_cond_signal(&condFreeSlot);

  // Increment indexToRead, given that I have just processed a record.
  indexToReadQ1++;
  indexToReadQ1 %= NUM_RECORDS;

  while ( !(records[indexToReadQ1].state & RECORD_READY_FOR_Q1) )
  {
#ifdef DEBUG_TIME
    wait_Q1_LP++;
    debugTimePause(&time_Q1, &started_Q1);
#endif
    //printf("Q1: blocking\n");
    pthread_cond_wait(&condAvailableRecordQ1, &mutex);
#ifdef DEBUG_TIME
    debugTimeCont(&started_Q1);
#endif
  }

  Record *record = &records[indexToReadQ1];

  pthread_mutex_unlock(&mutex);

  return record;
}



Record *RecordBuffer::getRecordQ2()
{
  pthread_mutex_lock(&mutex);

  if ( (records[indexToReadQ2].state &= ~RECORD_READY_FOR_Q2) == 0 &&
       (indexToWrite - indexToReadQ2 + NUM_RECORDS) % NUM_RECORDS < THRESHOLD_RECORDS)
    pthread_cond_signal(&condFreeSlot);

  // Increment indexToRead, given that I have just processed a record.
  indexToReadQ2++;
  indexToReadQ2 %= NUM_RECORDS;

  while ( !(records[indexToReadQ2].state & RECORD_READY_FOR_Q2) )
  {
#ifdef DEBUG_TIME
    wait_Q2_LP++;
    debugTimePause(&time_Q2, &started_Q2);
#endif
    //printf("Q2: blocking\n");
    pthread_cond_wait(&condAvailableRecordQ2, &mutex);
#ifdef DEBUG_TIME
    debugTimeCont(&started_Q2);
#endif
  }

  Record *record = &records[indexToReadQ2];

  pthread_mutex_unlock(&mutex);

  return record;
}
