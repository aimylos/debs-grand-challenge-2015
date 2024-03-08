#include <stdio.h>
#include <unistd.h>

#include "util/Timing.h"
#include "diskreader/BlockBuffer.h"
#include "linesplitter/LineBuffer.h"
#include "lineparser/RecordBuffer.h"


inline int subtract(int high, int low, int size)
{
  return (100 * ((high-low+size) % size)) / size;
}


void *Timing::thread_main(void *)
{
  while (true)
  {
    printf("%i\t%i\t%i\t%i\t\n",
      subtract(BlockBuffer::indexToWrite, BlockBuffer::indexToRead, NUM_BLOCKS),
      subtract(LineBuffer::indexToWrite, LineBuffer::indexToRead, NUM_LINES),
      subtract(RecordBuffer::indexToWrite, RecordBuffer::indexToReadQ1, NUM_RECORDS),
      subtract(RecordBuffer::indexToWrite, RecordBuffer::indexToReadQ2, NUM_RECORDS)
    );

    usleep(1000);
  }
}
