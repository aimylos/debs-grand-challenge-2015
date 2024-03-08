#ifndef LINE_SPLITTER_H_
#define LINE_SPLITTER_H_

#include "data.h"
#include "diskreader/DiskReader.h"
#include "linesplitter/LineBuffer.h"



class LineSplitter
{
  public: // fields
    static char *firstLineChar;
    static char *lastBlockChar;
    static char *curChar;
    static int lineCounter;
    static bool eof;

  private: // methods
    static void nextBlock();

  public:  // methods
    static void *thread_main(void *);
};

#endif /* LINE_SPLITTER_H_ */
