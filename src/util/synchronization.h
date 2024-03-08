#ifndef SYNCHRONIZATION_H_
#define SYNCHRONIZATION_H_

#include "parameters.h"

long getFastTimestamp();


#ifdef DEBUG_TIME
extern int wait_DR_LS;
extern int wait_LS_DR;

extern int wait_LS_LP;
extern int wait_LS_QR;

extern int wait_LP_LS;
extern int wait_LP_Q1;
extern int wait_LP_Q2;

extern int wait_Q1_LP;
extern int wait_Q2_LP;


extern long long started_DR;
extern long long started_LS;
extern long long started_LP;
extern long long started_Q1;
extern long long started_Q2;

extern long long time_DR;
extern long long time_LS;
extern long long time_LP;
extern long long time_Q1;
extern long long time_Q2;

void debugTimePause(long long *total, long long *started);
void debugTimeCont(long long *started);
#endif

#ifdef Q1_DEBUG_TIME
extern void printQ1Time();
#endif
#ifdef Q2_DEBUG_TIME
extern void printQ2Time();
#endif
#ifdef PARSER_TIME
extern void printParserTime();
#endif



#endif /* SYNCHRONIZATION_H_ */
