
#ifndef __DATA_H
#define __DATA_H

#include <time.h>

/**
 * TaxiId: The 128-bit md5sum of the vehicle ID. Uniquely identifies a taxi.
 * In the problem statement it is referred to as 'medallion'.
 */
typedef struct
{
  unsigned long long high;
  unsigned long long low;
} TaxiId;


/**
 * RouteId: A typedef defining RouteId is a 64-bit long long.
 */
typedef unsigned long long RouteId;


/**
 * RouteCells struct: Contains 4 fields of 16 bits each.
 * Each field represents a cell X or Y index.
 */
typedef struct
{
  unsigned short pickX;
  unsigned short pickY;
  unsigned short dropX;
  unsigned short dropY;
} RouteCells;

/**
 * Route union: It is 64-bit long. Can be interpreted either as a single
 * 64-bit number (long long) for use by the hash function, or as 4 16-bit
 * numbers (shorts).
 */
typedef union
{
  RouteId id;
  RouteCells cells;
} Route;



/**
 * Stores a timestamp as a struct.
 */
typedef struct
{
  char year;
  char month;
  char day;
  char hour;
  char min;
  char sec;
} Timestamp;


#define RECORD_READY_FOR_Q1  1
#define RECORD_READY_FOR_Q2  2
#define RECORD_READY_FOR_LP  4
#define RECORD_EOF           8

/**
 * Record struct: This struct contains all information we need to retain
 * per record (i.e., per line).
 */
typedef struct
{
  TaxiId taxi;        // The taxi id ("medallion" field of the data file)
  Route route300;     // This serves as the route id for Query 1
  Route route600;     // This serves as the route id for Query 2
  Timestamp dropoffTimestamp;
  long dropoffTime;   // Seconds since 1/1/1970
  int duration;       // Trip duration in seconds
  int profit;         // Fare+tip in cents (IGNORE tax, tolls, etc.)
  bool valid;
  char state;         // 0: empty,
                      // 4: ready for parsing
                      // 1,2: number of queries pending to consume it
                      // 8: Record indicating EOF. Ignore rest fields.

//  char *firstChar;    // NULL ==> this LineBoundary is "empty"
//  int blockIndex1;
//  int blockIndex2;

  clock_t startClockTick;  // CPU clock tick right *before* the LineSplitter processes this line.

#ifdef PRECISE_TIMING
  clock_t afterLS_comp;
  clock_t afterLS;
  clock_t beforeLP;
  clock_t afterLP_comp;
  clock_t afterLP;
  clock_t beforeQ1;
  clock_t afterQ1;
  clock_t beforeQ2;
  clock_t afterQ2;
#endif

} Record;


#endif  // __DATA_H
