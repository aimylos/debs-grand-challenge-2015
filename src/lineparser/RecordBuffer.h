#ifndef SRC_PARSER_RECORDBUFFER_H_
#define SRC_PARSER_RECORDBUFFER_H_

#include <pthread.h>

#include "data.h"
#include "parameters.h"


class RecordBuffer
{
  public:
    // fields
    static int indexToWrite;
    static int indexToReadQ1;
    static int indexToReadQ2;

    static pthread_mutex_t mutex;
    static pthread_cond_t condFreeSlot;
    static pthread_cond_t condAvailableRecordQ1;
    static pthread_cond_t condAvailableRecordQ2;

    static Record records[NUM_RECORDS];

    // methods
    static void init();
    static Record *getRecordLS();
    static void submitRecord(Record *record);
    static Record *getRecordQ1();
    static Record *getRecordQ2();

    static void inputIsComplete();
};

#endif /* SRC_PARSER_RECORDBUFFER_H_ */
