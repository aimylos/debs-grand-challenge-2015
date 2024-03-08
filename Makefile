CC = g++
CFLAGS = -std=c++11 -pthread -Wno-narrowing -O3 -DNDEBUG
EXEC = debs

SRC = src/util/synchronization.cc \
		src/util/Timing.cc \
		src/diskreader/DiskReader.cc \
		src/diskreader/BlockBuffer.cc \
		src/linesplitter/LineSplitter.cc \
		src/linesplitter/LineBuffer.cc \
		src/lineparser/LineParser.cc \
		src/lineparser/RecordBuffer.cc \
		src/parser/coordinates.cc \
		src/parser/mktime.cc \
		src/query1/dataStructQ1.cc \
		src/query1/dataStructQ1Output.cc \
		src/query2/dataStructQ2.cc \
		src/query2/dataStructQ2CellSorting.cc \
		src/query2/dataStructQ2Median.cc \
		src/query2/dataStructQ2Output.cc \
		src/main.cc
		
# SRC_TESTS = test/testDiskReader.cc

# Object targets by substituting .cc with .o
OBJS = $(subst .cc,.o,$(SRC))
# OBJS_TEST = $(subst .cc,.o,$(SRC_TESTS))

# Include files
INC = -I./src/

# Build .o objects
.cc.o:
	${CC} ${CFLAGS} ${INC} -c $< -o $@


all: ${EXEC}


# Compile objects into executable
${EXEC}: ${OBJS}
	${CC} -pthread -o $@ ${OBJS}

#testDiskReader: ${OBJS} ${OBJS_TEST}
#	${CC} 


clean:
	rm -f ${EXEC} ${OBJS}
# rm ${OBJS} ${OBJS_TEST}