#ifndef SRC_PARSER_LINEBUFFER_H_
#define SRC_PARSER_LINEBUFFER_H_

#include <pthread.h>

#include "data.h"
#include "parameters.h"




typedef struct
{
  char *firstChar; // NULL ==> this LineBoundary is "empty"
  Record *record;
  int blockIndex1;
  int blockIndex2;
} Line;



class LineBuffer
{
  public:
    // fields
    static Line lines[NUM_LINES];
    static int indexToRead;
    static int indexToWrite;
    // if indexToRead == indexToWrite, then the buffer is EMPTY.

    static pthread_mutex_t mutex;
    static pthread_cond_t condNotEmpty;
    static pthread_cond_t condNotFull;

    // methods
    static void init();
    static void producedLine();
    static Line *getNextLine();
    static void test();
};

#endif /* SRC_PARSER_LINEBUFFER_H_ */
