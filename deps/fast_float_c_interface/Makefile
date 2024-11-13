CXX?=c++
# FASTFLOAT_ALLOWS_LEADING_PLUS allows +inf to be parsed as inf, instead of error. 
CXXFLAGS=-std=c++11 -fPIC -D FASTFLOAT_ALLOWS_LEADING_PLUS

.PHONY: all clean
all: valkey_strtod.o
clean:
	rm -f *.o || true;

# src/Makefile passes values for OPT, STD and WARN variables. These are core CFLAGS used by executable targets.
# We don't need to pass USE_FAST_FLOAT value to compiler here. The code in src/ includes the header files as is,
# and at that point disables fast_float code based on USE_FAST_FLOAT value. Therefore, only src/Makefile passes 
# USE_FAST_FLOAT value to compiler.
valkey_strtod.o: valkey_strtod.cpp valkey_strtod.h
	$(CXX) $(OPT) $(STD) $(WARN) $(CXXFLAGS) -o $@ -c $<
