CXX?=g++
CXXFLAGS=-std=c++11 -O3 -fPIC -D FASTFLOAT_ALLOWS_LEADING_PLUS

SOURCES=fast_float_strtod.cpp

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -c $< -o $@

.PHONY: all clean test

all: fast_float_strtod.o

clean:
	rm -f *.o || true;
	rm test || true;

test: fast_float_strtod.o
	$(CC) test_fast_float.c -c -o test_fast_float.o	
	$(CXX) test_fast_float.o fast_float_strtod.o -o test
	./test
