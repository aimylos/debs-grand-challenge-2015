#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>

#include "data.h"
#include "parameters.h"

#include "diskreader/DiskReader.h"
#include "diskreader/BlockBuffer.h"
#include "lineparser/parser_includes.h"
#include "query1/dataStructQ1.h"
#include "query2/dataStructQ2.h"
#include "util/Timing.h"
#include "util/synchronization.h"




void startThreads()
{
  pthread_attr_t attr;
  pthread_t diskreader;
  pthread_t line_splitter;
  pthread_t line_parser;
  pthread_t query1;
  pthread_t query2;

#ifdef DEBUG_TIME
  debugTimeCont(&started_DR);
  debugTimeCont(&started_LS);
  debugTimeCont(&started_LP);
  debugTimeCont(&started_Q1);
  debugTimeCont(&started_Q2);
#endif


  pthread_attr_init(&attr);

#ifdef BUFFERS_SAMPLING
  pthread_t sampling;
  pthread_create(&sampling, &attr, Timing::thread_main, NULL);
#endif

  pthread_create(&diskreader, &attr, DiskReader::thread_main, NULL);
  pthread_create(&line_splitter, &attr, LineSplitter::thread_main, NULL);
  pthread_create(&line_parser, &attr, LineParser::thread_main, NULL);
//  pthread_create(&line_parser, &attr, LineParser::thread_main, NULL);
  pthread_create(&query1, &attr, DataStructQ1::query1_thread_main, NULL);
  pthread_create(&query2, &attr, DataStructQ2::query2_thread_main, NULL);

#ifdef Q1_DO_OUTPUT_THREAD
  pthread_t outputThreadQ1;
  pthread_create(&outputThreadQ1, &attr, DataStructQ1::outputThread_main, NULL);
#endif
#ifdef Q2_DO_OUTPUT_THREAD
  pthread_t outputThreadQ2;
  pthread_create(&outputThreadQ2, &attr, DataStructQ2::outputThread_main, NULL);
#endif

  pthread_join(query1, NULL);
  pthread_join(query2, NULL);

  #ifdef Q1_DO_OUTPUT_THREAD
  pthread_join(outputThreadQ1, NULL);
  #endif
    
  #ifdef Q2_DO_OUTPUT_THREAD
  pthread_join(outputThreadQ2, NULL);
  #endif
}



int main(int argc, char **argv)
{
  char *inputFilename;
  char *outputFilename1;
  char *outputFilename2;

//  printf("time_t = %i %i\n", sizeof(time_t), sizeof(long int));

  if (argc == 4)
  {
    inputFilename = argv[1];
    outputFilename1 = argv[2];
    outputFilename2 = argv[3];
  }
  else if (argc == 3)
  {
    inputFilename = argv[1];
    int len = strlen(argv[2]) + 6;
    outputFilename1 = (char *)malloc(len);
    outputFilename2 = (char *)malloc(len);
    sprintf(outputFilename1, "%s1.out", argv[2]);
    sprintf(outputFilename2, "%s2.out", argv[2]);
  }
  else
  {
    fprintf(stderr, "Syntax: %s <inFile> <outFile1> <outFile2>\n"
                    "    or: %s <inFile> <outFileBase>\n\n", argv[0], argv[0]);
    return -1;
  }


  DiskReader::setFilename(inputFilename);

  BlockBuffer::init();
  RecordBuffer::init();

  DataStructQ1::dataStructQ1InitFast(outputFilename1);
  DataStructQ2::dataStructQ2InitFast(outputFilename2);

  startThreads();
  
  //LineBuffer::test();


#ifdef DEBUG_TIME
//  debugTimePause(&time_DR, &started_DR);
//  debugTimePause(&time_LS, &started_LS);
//  debugTimePause(&time_LP, &started_LP);
//  debugTimePause(&time_Q1, &started_Q1);
//  debugTimePause(&time_Q2, &started_Q2);

  fprintf(stderr, "DR <--> LS       %i\t%i\n", wait_DR_LS, wait_LS_DR);
  fprintf(stderr, "LS <--> LP       %i\t%i\n", wait_LS_LP, wait_LP_LS);
  fprintf(stderr, "LP <--> Q1       %i\t%i\n", wait_LP_Q1, wait_Q1_LP);
  fprintf(stderr, "LP <--> Q2       %i\t%i\n", wait_LP_Q2, wait_Q2_LP);
  fprintf(stderr, "LS  --> QR       %i\n", wait_LS_QR);

  fprintf(stderr, "\n");
  fprintf(stderr, "runtime DR --> %lli\n", time_DR);
  fprintf(stderr, "runtime LS --> %lli\n", time_LS);
  fprintf(stderr, "runtime LP --> %lli\n", time_LP);
  fprintf(stderr, "runtime Q1 --> %lli\n", time_Q1);
  fprintf(stderr, "runtime Q2 --> %lli\n", time_Q2);
#endif


#ifdef Q1_COMPILE_DEBUG_CODE
  DataStructQ1::printOuputMessages();
#endif

#ifdef Q2_COMPILE_DEBUG_CODE
  DataStructQ2::printOuputMessages();
#endif
  
  return 0;
}
