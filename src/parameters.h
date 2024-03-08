
#ifndef PARAMETERS_H_
#define PARAMETERS_H_


/* Disk Reader */
//#################################################//
#define BLOCK_SIZE        16384
#define NUM_BLOCKS         1000
#define THRESHOLD_BLOCKS     10

/* Line Splitter */
//#################################################//
#define NUM_LINES 70
#define THRESHOLD_LINES 70
#define MIN_LINE_LENGTH     180   // Should be at most 182. The lower the safer.


/* Parser */
//#################################################//
#define NUM_RECORDS 70
#define THRESHOLD_RECORDS 25


/* Q1 */
//#################################################//
#define Q1_OUTPUT_STREAM
// #define Q1_DO_OUTPUT_THREAD


/* Q2 */
//#################################################//
#define Q2_OUTPUT_STREAM
// #define Q2_DO_OUTPUT_THREAD



/* DEBUGGING AND TIMING */
//#################################################//

//#define DEBUG_TIME
//#define PRECISE_TIMING
//#define BUFFERS_SAMPLING

//#define Q1_DEBUG_TIME
//#define Q2_DEBUG_TIME
//#define PARSER_TIME

//debug Q1 -- uncomment Q1_COMPILE_DEBUG_CODE
//#define Q1_COMPILE_DEBUG_CODE
//#define Q1_TIMING


//debug Q2 -- uncomment Q2_COMPILE_DEBUG_CODE
//#define Q2_COMPILE_DEBUG_CODE
//#define Q2_TIMING


#endif /* PARAMETERS_H_ */
