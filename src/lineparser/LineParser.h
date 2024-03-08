#ifndef LINE_PARSER_H_
#define LINE_PARSER_H_

#include "data.h"
#include "parameters.h"
#include "diskreader/BlockBuffer.h"




class LineParser
{
  private:
    // methods
    static int readInt(char *&p);
    static int readFloat(char *&p);
    static long readDate(Record *&r, char *&p);

  public:
    // methods
    static void *thread_main(void *);
    static void printRecord(Record *r);
};

#endif /* LINE_PARSER_H_ */
