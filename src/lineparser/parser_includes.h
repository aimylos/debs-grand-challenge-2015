#ifndef SRC_LINEPARSER_PARSER_INCLUDES_H_
#define SRC_LINEPARSER_PARSER_INCLUDES_H_

#ifdef NEW_PARSER
#include "lineparser/LineParserNew.h"
#include "lineparser/RecordBufferNew.h"
#include "linesplitter/LineSplitterNew.h"
#else
#include "lineparser/LineParser.h"
#include "linesplitter/LineBuffer.h"
#include "lineparser/RecordBuffer.h"
#include "linesplitter/LineSplitter.h"
#endif

#endif /* SRC_LINEPARSER_PARSER_INCLUDES_H_ */
