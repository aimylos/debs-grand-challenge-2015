#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>

#include "diskreader/DiskReader.h"
#include "diskreader/BlockBuffer.h"
#include "util/synchronization.h"


char *DiskReader::filename;


void DiskReader::setFilename(char *name)
{
  filename = name;
}


/**
 * Main loop of the disk reader.
 */
void *DiskReader::thread_main(void *)
{
  int fd = open(filename, O_RDONLY);
  int bytes;

  while (true)
  {
    // Here we are certain that buffer[indexToWrite] is ready to be written.
    bytes = read(fd, BlockBuffer::buffer + BLOCK_SIZE*BlockBuffer::indexToWrite, BLOCK_SIZE);

    //if (bytes < 16384)
    //  printf("bytes = %i\n", bytes);

    //printf("Read %i chars in buffer[%i]\n", bytes, BlockBuffer::indexToWrite);
    BlockBuffer::lastChar[BlockBuffer::indexToWrite] = BlockBuffer::buffer + BLOCK_SIZE*BlockBuffer::indexToWrite + bytes;

    //printf("DR: %i\n",BlockBuffer::indexToWrite);
    BlockBuffer::producedBlock();

    if (bytes==0)
      pthread_exit(NULL);
  }

  return NULL;
}
