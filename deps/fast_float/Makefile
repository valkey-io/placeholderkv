CXX?=g++
CXXFLAGS=-std=c++11 -O3 -fPIC

SOURCES=fast_float_strtod.cpp

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -c $< -o $@

.PHONY: all clean

all: fast_float_strtod.o

clean:
	rm -f *.o 
