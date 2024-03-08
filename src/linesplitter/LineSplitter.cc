#include <stdio.h>
#include <string.h>
#include <pthread.h>

#include <assert.h>

#include "util/synchronization.h"
#include "diskreader/BlockBuffer.h"
#include "linesplitter/LineSplitter.h"
#include "linesplitter/LineBuffer.h"
#include "lineparser/RecordBuffer.h"




char *LineSplitter::firstLineChar = NULL;
char *LineSplitter::lastBlockChar = NULL;
char *LineSplitter::curChar = NULL;
int LineSplitter::lineCounter = 0;
bool LineSplitter::eof = false;




inline void LineSplitter::nextBlock()
{
  BlockBuffer::waitForAvailableBlock();

  // If we are wrapping around the buffer array, copy the leftover of the
  // current line to block 0's prefix.
  if (BlockBuffer::indexToRead == 0)
  {
    // copy the leftover from the previous block to the new block's prefix.
    int leftover = lastBlockChar - firstLineChar;
    //printf("LEFTOVER: %i\n", leftover);
    char *newFirstLineChar = BlockBuffer::buffer - leftover;
    memcpy(newFirstLineChar, firstLineChar, leftover);
    curChar -= firstLineChar - newFirstLineChar;
    firstLineChar = newFirstLineChar;
  }
  lastBlockChar = BlockBuffer::lastChar[BlockBuffer::indexToRead];

  // Check if this is an empty block, therefore we have reached the end of the file.
  if (lastBlockChar == BlockBuffer::buffer + BLOCK_SIZE * BlockBuffer::indexToRead)
    eof = true;
}



//void printAddr(char *prefix)
//{
//  printf("%s  %lX  %lX  %lX  %lX  %lX  %lX  %lX  %lX\n",
//      prefix,
//      BlockBuffer::totalBuffer[0],
//      BlockBuffer::buffer,
//      BlockBuffer::buffer+BLOCK_SIZE*BlockBuffer::indexToRead,
//      BLOCK_SIZE*BlockBuffer::indexToRead,
//      LineSplitter::firstLineChar - (BlockBuffer::buffer+BLOCK_SIZE*BlockBuffer::indexToRead),
//      LineSplitter::lastBlockChar,
//      LineSplitter::firstLineChar,
//      LineSplitter::curChar
//      );
//  fflush(stdout);
//}



void *LineSplitter::thread_main(void *)
{
  // fetch the first block
  nextBlock();
  firstLineChar = BlockBuffer::buffer;
  lineCounter = 0;

  while (true)
  {
    Line *line = &LineBuffer::lines[LineBuffer::indexToWrite];

    // Get next empty Record index
    line->record = RecordBuffer::getRecordLS();

    // Store the CPU clock tick when this line starts being processed
    //line->record->startClockTick = clock();
    line->record->startClockTick = getFastTimestamp();

    line->blockIndex1 = BlockBuffer::indexToRead;
    lineCounter++;
    line->blockIndex2 = -1;  // no second block involved in this line, yet.

    if (lastBlockChar - curChar <= MIN_LINE_LENGTH)
    {
      BlockBuffer::splitLinesOfBlock(BlockBuffer::indexToRead, lineCounter);
      lineCounter = 0;
      nextBlock();
      line->blockIndex2 = BlockBuffer::indexToRead;
      lineCounter++;
    }

    if (eof)
    {
      line->firstChar = (char *)-1;  // line indicating the EOF
      LineBuffer::producedLine();
      pthread_exit(NULL);
    }

    curChar += MIN_LINE_LENGTH;

    while (*curChar != '\n')
    {
      if (++curChar == lastBlockChar)
      {
        BlockBuffer::splitLinesOfBlock(BlockBuffer::indexToRead, lineCounter);
        lineCounter = 0;
        nextBlock();
        assert(line->blockIndex2 == -1); // && "More than two blocks for this line???");
        line->blockIndex2 = BlockBuffer::indexToRead;
        lineCounter++;
      }
    }

    // Add new line's boundaries in the array
    line->firstChar = firstLineChar;

    // Proceed curChar to the first character of the next line, and store it in firstLineChar.
    firstLineChar = ++curChar;

#ifdef PRECISE_TIMING
    line->record->afterLS_comp = clock();
#endif

    // Notify Q1 & Q2, and increment RecordBuffer::indexToWrite
    LineBuffer::producedLine();

#ifdef PRECISE_TIMING
    line->record->afterLS = clock();
#endif
  }
}
