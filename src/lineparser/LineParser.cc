#include <stdio.h>
#include <string.h>
#include <pthread.h>

#include "data.h"
#include "diskreader/BlockBuffer.h"
#include "linesplitter/LineBuffer.h"
#include "lineparser/LineParser.h"
#include "lineparser/RecordBuffer.h"
#include "parser/mktime.h"
#include "util/synchronization.h"


//COORDINATES
#define X_500m            (5986)
#define X_250m            (X_500m/2)
#define X_MIN_CENTER      (-74913585)
#define X300_MIN          (X_MIN_CENTER - X_500m/2)
#define X300_MAX          (X300_MIN + 300*X_500m)
#define X600_MIN          (X_MIN_CENTER - X_250m/2)

#define Y_500m            (4491.556)  // :1000
#define Y_250m            (Y_500m/2)    // :1000
#define Y_MAX_CENTER      (41474937)
#define Y300_MAX          (Y_MAX_CENTER + Y_500m/2)
#define Y300_MIN          (Y300_MAX - 300*Y_500m)
#define Y600_MAX          (Y_MAX_CENTER + Y_250m/2)

#define INVALIDX_PICKX (pickX)<X300_MIN || (pickX)>X300_MAX
#define INVALIDX_DROPX (dropX)<X300_MIN || (dropX)>X300_MAX
#define INVALIDY_PICKY (pickY)<Y300_MIN || (pickY)>Y300_MAX
#define INVALIDY_DROPY (dropY)<Y300_MIN || (dropY)>Y300_MAX

#define GETCELLX300_PICKX (pickX - X300_MIN) / X_500m;
#define GETCELLX300_DROPX (dropX - X300_MIN) / X_500m;

#define GETCELLY300_PICKY (Y300_MAX - pickY) / Y_500m;
#define GETCELLY300_DROPY (Y300_MAX - dropY) / Y_500m;

#define GETCELLX600_PICKX (pickX-X600_MIN) / X_250m;
#define GETCELLX600_DROPX (dropX-X600_MIN) / X_250m;

#define GETCELLY600_PICKY (Y600_MAX - pickY) / Y_250m;
#define GETCELLY600_DROPY (Y600_MAX - dropY) / Y_250m;
//END OF COORDINATES


/**
 * Reads an int.
 */
inline int LineParser::readInt(char *&p)
{
  int k;

  k = 0;
  while (*p != ',')
  {
    k = 10 * k + *p - '0';
    p++;
  }
  p++;

  return k;
}



/**
 * Reads a float number as int. That is, the period (.) is just ignored.
 */
inline int LineParser::readFloat(char *&p)
{
  int k;
  bool negative;

  k = 0;
  negative = false;

  if (*p == '-')
  {
    negative = true;
    p++;
  }

  while (*p != ',')
  {
    if (*p != '.')
      k = 10 * k + *p - '0';
    p++;
  } 
  p++;

  return negative ? -k : k;
}



/**
 * Reads a date, and returns the number of seconds since 1970.
 */
inline long LineParser::readDate(Record *&r, char *&p)
{
  r->dropoffTimestamp.year = 1000 * p[0] + 100 * p[1] + 10 * p[2] + p[3] - 1111 * '0' - 1970;
  r->dropoffTimestamp.month = 10 * p[5] + p[6] - 11 * '0' - 1; // 0..11 instead of 1..12
  r->dropoffTimestamp.day =  10 * p[8] + p[9] - 11 * '0';
  r->dropoffTimestamp.hour = 10 * p[11] + p[12] - 11 * '0';
  r->dropoffTimestamp.min =  10 * p[14] + p[15] - 11 * '0';
  r->dropoffTimestamp.sec =  10 * p[17] + p[18] - 11 * '0';

  p += 20;

  r->dropoffTime = mkTime[r->dropoffTimestamp.year * 12
                          + r->dropoffTimestamp.month]
                   + r->dropoffTimestamp.day * 86400
                   + r->dropoffTimestamp.hour * 3600
                   + r->dropoffTimestamp.min * 60
                   + r->dropoffTimestamp.sec;
}



// void LineParser::printRecord(Record *r)
// {
//   printf("%016llX%016llX\t", r->taxi.high, r->taxi.low);
//   printf("%i.%i\t", r->route300.cells.pickX, r->route300.cells.pickY);
//   printf("%i.%i\t", r->route300.cells.dropX, r->route300.cells.dropY);
//   printf("%li\t", r->dropoffTime);
//   printf("%i\t", r->duration);
//   printf("%i\t", r->profit);
//   printf("%s", r->valid ? "valid" : "invalid");
//   printf("\n");
// }



/**
 * This is the main loop of the parser thread.
 */
void *LineParser::thread_main(void *)
{
  int pickX, pickY, dropX, dropY;
  Record *record;
  Line *line;
  long long taxiId;
  char *p, *q;

  while (true)
  {
    line = LineBuffer::getNextLine();
    record = line->record;

    if (line->firstChar == (char *)-1)
    {
      record->valid = false;
      record->state |= RECORD_EOF;  // Indicate the EOF has been reached. Ignore rest fields.
      RecordBuffer::submitRecord(record);
      pthread_exit(NULL);
    }

    record->valid = true;

#ifdef PRECISE_TIMING
    record->beforeLP = clock();
#endif

    //go to duration
    p = line->firstChar + 106;

    // read duration
    record->duration = readInt(p);

    // go to pickup_longitude
    while (*p++ != ',');  
    
    // Pickup longitude (X)
    pickX = readFloat(p);
    if ( INVALIDX_PICKX ) {record->valid = false; goto L1;}

    // Pickup longitude (Y)
    pickY = readFloat(p);
    if ( INVALIDY_PICKY ) {record->valid = false; goto L1;}

    // Drop-off longitude (X)
    dropX = readFloat(p);
    if ( INVALIDX_DROPX ) {record->valid = false; goto L1;}

    // Drop-off longitude (Y)
    dropY = readFloat(p);
    if ( INVALIDY_DROPY ) {record->valid = false; goto L1;}

    //get cell indexes
    record->route300.cells.pickX = GETCELLX300_PICKX
    record->route600.cells.pickX = GETCELLX600_PICKX
    record->route300.cells.pickY = GETCELLY300_PICKY
    record->route600.cells.pickY = GETCELLY600_PICKY

    record->route300.cells.dropX = GETCELLX300_DROPX
    record->route600.cells.dropX = GETCELLX600_DROPX
    record->route300.cells.dropY = GETCELLY300_DROPY
    record->route600.cells.dropY = GETCELLY600_DROPY

    // go to fare_amount
    while (*p++ != ',');

    //read fare
    record->profit = readFloat(p);

    // go to mta_tax
    while (*p++ != ',');
    // go to tip_amount
    while (*p++ != ',');
    
    //read tip
    record->profit += readFloat(p);

    //go to line start
    p = line->firstChar;

    //read taxi id (medallion)
    taxiId = 0;
    q = p;
    for (; p<q+16; p++)
      taxiId = taxiId<<4 | (*p<='9' ? *p-'0' : *p-'A'+10);
    record->taxi.high = taxiId;

    p += 70;

    readDate(record, p);

    L1:
    //printRecord(record);
#ifdef PRECISE_TIMING
    record->afterLP_comp = clock();
#endif
    RecordBuffer::submitRecord(record);

    // Tell the BlockBuffer that I parsed yet another line of this block (or these blocks).
    BlockBuffer::parsedLineOfBlock(line->blockIndex1);
    if (line->blockIndex2 >= 0)
      BlockBuffer::parsedLineOfBlock(line->blockIndex2);

    // mark the line as empty
    line->firstChar = NULL;

#ifdef PRECISE_TIMING
    record->afterLP = clock();
#endif

  }

  return NULL;
}
