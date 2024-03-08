#include <pthread.h>
#include <stdio.h>
#include <sys/time.h>

#include "util/synchronization.h"

#include "query2/dataStructQ2.h"



#ifdef DEBUG_TIME
int wait_DR_LS = 0;
int wait_LS_DR = 0;

int wait_LS_LP = 0;
int wait_LS_QR = 0;

int wait_LP_LS = 0;
int wait_LP_Q1 = 0;
int wait_LP_Q2 = 0;

int wait_Q1_LP = 0;
int wait_Q2_LP = 0;


long long started_DR = 0;
long long started_LS = 0;
long long started_LP = 0;
long long started_Q1 = 0;
long long started_Q2 = 0;

long long time_DR = 0;
long long time_LS = 0;
long long time_LP = 0;
long long time_Q1 = 0;
long long time_Q2 = 0;



void debugTimePause(long long *total, long long *started)
{
  long long now = getFastTimestamp();
  *total += now-*started;
//  fprintf(stderr, "%li\n", now);
}

void debugTimeCont(long long *started)
{
  *started = getFastTimestamp();
}
#endif


long getFastTimestamp()
{
  timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return 1000000*(ts.tv_sec) + ts.tv_nsec/1000;  // use microsec
  //return ((ts.tv_sec & 0xFFFFFFFF) << 32) | (ts.tv_nsec >> 32);
}


//void syncInit()
//{
//  // Diskreader <-> Parser
//  pthread_mutex_init(&ioMutex, NULL);
//  pthread_cond_init(&ioCondVar, NULL);
//
//  ioFirstEmpty = 0;
//  ioFirstFull = NUM_BLOCKS_OLD-1;
//
//
//  // Parser <-> Queries
//  pthread_mutex_init(&recMutex, NULL);
//  pthread_cond_init(&recCondVarQ1, NULL);
//  pthread_cond_init(&recCondVarQ2, NULL);
//
//  recFirstEmpty = 0;
//  recFirstFullQ1 = RECORDS_ARRAY-1;
//  recFirstFullQ2 = RECORDS_ARRAY-1;
//}
//
//
//
//void ioProducedBlock()
//{
//  pthread_mutex_lock(&ioMutex);  // enter CR
//
//  // First, increment ioFirstEmpty
//  ioFirstEmpty++;
//  ioFirstEmpty %= NUM_BLOCKS_OLD;
//
//  // Notify the parser that a new block has been read
//  pthread_cond_signal(&ioCondVar);
//
//  // And then, check if the buffer is full, i.e., ioFirstEmpty + 1 == firstFull
//  while ( (ioFirstEmpty+1) % NUM_BLOCKS_OLD == ioFirstFull)
//  {
//#ifdef DEBUG_TIME
//    waitDiskreaderParser++;
//    debugTimePause(&timeDiskreader, &startedDiskreader);
//#endif
//    pthread_cond_wait(&ioCondVar, &ioMutex);
//#ifdef DEBUG_TIME
//    debugTimeCont(&startedDiskreader);
//#endif
//  }
//
//  pthread_mutex_unlock(&ioMutex);  // exit CR
//}
//
//
//
//void ioConsumedBlock()
//{
//  pthread_mutex_lock(&ioMutex);
//
//  // Increment ioFirstFull, given that I have just processed a block.
//  ioFirstFull++;
//  ioFirstFull %= NUM_BLOCKS_OLD;
//
//  // Notify the disk reader that I have processed the previous block
//  pthread_cond_signal(&ioCondVar);
//
//  // Check if the buffer is empty
//  while (ioFirstEmpty == ioFirstFull)
//  {
//#ifdef DEBUG_TIME
//    waitParserDiskreader++;
//    debugTimePause(&timeParser, &startedParser);
//#endif
//    pthread_cond_wait(&ioCondVar, &ioMutex);
//#ifdef DEBUG_TIME
//    debugTimeCont(&startedParser);
//#endif
//  }
//
//  pthread_mutex_unlock(&ioMutex);
//}
//
//
//
//void recProducedRecord()
//{
//  pthread_mutex_lock(&recMutex);  // enter CR
//
//  // First, increment ioFirstEmpty
//  recFirstEmpty++;
//  recFirstEmpty %= RECORDS_ARRAY;
//
//  // Notify the query threads that a new record has been produced
//  pthread_cond_signal(&recCondVarQ1);
//  pthread_cond_signal(&recCondVarQ2);
//
//  // And then, check if the buffer is full, i.e., ioFirstEmpty + 1 == firstFull
//  while ( (recFirstEmpty+1) % RECORDS_ARRAY == recFirstFullQ1)
//  {
//#ifdef DEBUG_TIME
//    waitParserQ1++;
//    debugTimePause(&timeParser, &startedParser);
//#endif
//    pthread_cond_wait(&recCondVarQ1, &recMutex);
//#ifdef DEBUG_TIME
//    debugTimeCont(&startedParser);
//#endif
//  }
//
//  while ( (recFirstEmpty+1) % RECORDS_ARRAY == recFirstFullQ2)
//  {
//#ifdef DEBUG_TIME
//    waitParserQ2++;
//    debugTimePause(&timeParser, &startedParser);
//#endif
//    pthread_cond_wait(&recCondVarQ2, &recMutex);
//#ifdef DEBUG_TIME
//    debugTimeCont(&startedParser);
//#endif
//  }
//
//  pthread_mutex_unlock(&recMutex);  // exit CR
//}
//
//
//
//void recConsumedRecordQ1()
//{
//  pthread_mutex_lock(&recMutex);
//
//  // Increment ioFirstFull, given that I have just processed a block.
//  recFirstFullQ1++;
//  recFirstFullQ1 %= RECORDS_ARRAY;
//
//  // Notify the disk reader that I have processed the previous block
//  pthread_cond_signal(&recCondVarQ1);
//
//  while (recFirstEmpty == recFirstFullQ1)
//  {
//#ifdef DEBUG_TIME
//    waitQ1Parser++;
//    debugTimePause(&timeQ1, &startedQ1);
//#endif
//    if (inputCompleted)
//    {
//      //printf("Exiting 1");
//      pthread_mutex_unlock(&recMutex);
//      pthread_exit(NULL);
//    }
//    pthread_cond_wait(&recCondVarQ1, &recMutex);
//#ifdef DEBUG_TIME
//    debugTimeCont(&startedQ1);
//#endif
//  }
//
//  pthread_mutex_unlock(&recMutex);
//}
//
//
//
//void recConsumedRecordQ2()
//{
//  pthread_mutex_lock(&recMutex);
//
//  // Increment ioFirstFull, given that I have just processed a block.
//  recFirstFullQ2++;
//  recFirstFullQ2 %= RECORDS_ARRAY;
//
//  // Notify the disk reader that I have processed the previous block
//  pthread_cond_signal(&recCondVarQ2);
//
//  while (recFirstEmpty == recFirstFullQ2)
//  {
//#ifdef DEBUG_TIME
//    waitQ2Parser++;
//    debugTimePause(&timeQ2, &startedQ2);
//#endif
//    if (inputCompleted)
//    {
//      //printf("Exiting 2");
//      pthread_mutex_unlock(&recMutex);
//      pthread_exit(NULL);
//    }
//    pthread_cond_wait(&recCondVarQ2, &recMutex);
//#ifdef DEBUG_TIME
//    debugTimeCont(&startedQ2);
//#endif
//  }
//
//  pthread_mutex_unlock(&recMutex);
//}
//
//
//
void recProducerExit()
{
//  pthread_mutex_lock(&recMutex);
//
//  inputCompleted = true;
//
//  pthread_cond_signal(&recCondVarQ1);
//  pthread_cond_signal(&recCondVarQ2);
//
//  pthread_mutex_unlock(&recMutex);


#ifdef Q1_DEBUG_TIME
  printQ1Time();
#endif

#ifdef Q2_DEBUG_TIME
  DataStructQ2::printTimings();
  printQ2Time();
#endif

#ifdef PARSER_TIME
  printParserTime();
#endif



//  pthread_exit(NULL);
}

