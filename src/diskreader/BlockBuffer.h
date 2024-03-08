#ifndef SRC_DISKREADER_BLOCKBUFFER_H_
#define SRC_DISKREADER_BLOCKBUFFER_H_


#include <pthread.h>

#include "parameters.h"


typedef struct
{
  bool filled;
  bool allLinesSplit;
  int linesTotal;
  int linesParsed;
} BlockState;

class BlockBuffer
{
  public:
    // fields
    static int indexToWrite;
    static int indexToRead;

    static pthread_mutex_t mutex;
    static pthread_cond_t condAvailableBlock;
    static pthread_cond_t condEmptyBlock;

    static char totalBuffer[NUM_BLOCKS+1][BLOCK_SIZE];
    static char *buffer;
    static char *lastChar[NUM_BLOCKS];

    static BlockState state[NUM_BLOCKS];

    static bool inputCompleted;

    //methods
    static void init();
    static void producedBlock();
    static void waitForAvailableBlock();
    static void splitLinesOfBlock(int blockIndex, int numLines);
    static void parsedLineOfBlock(int blockIndex);
};

#endif /* SRC_DISKREADER_BLOCKBUFFER_H_ */
