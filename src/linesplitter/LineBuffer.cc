#include <pthread.h>

#include "linesplitter/LineBuffer.h"
#include "util/synchronization.h"
#include <stdio.h>


// Debugging
//#include <stdlib.h>
//#include <unistd.h>
//void printState(char owner)
//{
//  printf("%c: w=%i  r=%i  ", owner, LineBuffer::indexToWrite, LineBuffer::indexToRead);
//  for (int i=0; i<NUM_LINES; i++)
//    printf("%c ", LineBuffer::lines[i].firstChar == NULL ? '0' : '1');
//  printf("\n");
//}



Line LineBuffer::lines[NUM_LINES];
int LineBuffer::indexToRead = 0;
int LineBuffer::indexToWrite = 0;

pthread_mutex_t LineBuffer::mutex;
pthread_cond_t LineBuffer::condNotEmpty;
pthread_cond_t LineBuffer::condNotFull;


void LineBuffer::init()
{
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&condNotFull, NULL);
  pthread_cond_init(&condNotEmpty, NULL);
}



void LineBuffer::producedLine()
{
  pthread_mutex_lock(&mutex);

  // Increment indexToWrite
  indexToWrite++;
  indexToWrite %= NUM_LINES;

  // Notify one LineParser thread waiting for a line
  pthread_cond_signal(&condNotEmpty);

  // if indexToWrite == indexToRead, then the buffer is EMPTY.
  // Check if the buffer is full.
  while ( (indexToWrite+1) % NUM_LINES == indexToRead ||
          lines[indexToWrite].firstChar != NULL )
  {
#ifdef DEBUG_TIME
    wait_LS_LP++;
    debugTimePause(&time_LS, &started_LS);
#endif
    pthread_cond_wait(&condNotFull, &mutex);
#ifdef DEBUG_TIME
    debugTimeCont(&started_LS);
#endif
  }

  pthread_mutex_unlock(&mutex);
}



Line *LineBuffer::getNextLine()
{
  pthread_mutex_lock(&mutex);

  // Notify LineSplitter that I arrived here.
  if ( (indexToWrite - indexToRead + NUM_LINES) % NUM_LINES < THRESHOLD_LINES)
    pthread_cond_signal(&condNotFull);

  // Block if the buffer is empty
  while (indexToRead == indexToWrite)
  {
#ifdef DEBUG_TIME
    wait_LP_LS++;
    debugTimePause(&time_LP, &started_LP);
#endif
    pthread_cond_wait(&condNotEmpty, &mutex);
#ifdef DEBUG_TIME
    debugTimeCont(&started_LP);
#endif
  }

  Line *line = &lines[indexToRead];

  // Increment indexToRead
  indexToRead++;
  indexToRead %= NUM_LINES;

  pthread_mutex_unlock(&mutex);

  return line;
}



void *p(void *)
{
  int index = 1000;
  while (true)
  {
    LineBuffer::lines[LineBuffer::indexToWrite].blockIndex1 = index;
    printf("---> %i  (%i-%i)\n", index, LineBuffer::indexToWrite, LineBuffer::indexToRead);
    index++;
    //usleep( rand() % 10);
    LineBuffer::lines[LineBuffer::indexToWrite].firstChar = (char *)-1;
    //usleep( rand() % 10);
    LineBuffer::producedLine();
  }
}

void *c(void *pointer)
{
  char id = (long)pointer & 0xFF;

  while (true)
  {
    Line *line = LineBuffer::getNextLine();
    printf("     %i (%c) <---\n", line->blockIndex1, (char)id);
    //printState('c');
    //usleep( rand() % 1000);
    LineBuffer::lines[LineBuffer::indexToWrite].firstChar = NULL;
    //usleep( rand() % 1000);
  }
}

void LineBuffer::test()
{
  pthread_attr_t attr;
  pthread_t t1, t2;

  pthread_attr_init(&attr);
  pthread_create(&t1, &attr, p, NULL);
  pthread_create(&t2, &attr, c, (void *)'x');
  pthread_create(&t2, &attr, c, (void *)'y');
  pthread_create(&t2, &attr, c, (void *)'z');
  pthread_create(&t2, &attr, c, (void *)'w');

  pthread_join(t1, NULL);
}
