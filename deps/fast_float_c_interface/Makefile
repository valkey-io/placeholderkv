CCCOLOR="\033[34m"
SRCCOLOR="\033[33m"
ENDCOLOR="\033[0m"

ifndef V
	QUIET_CXX = @printf '    %b %b\n' $(CCCOLOR)C++$(ENDCOLOR) $(SRCCOLOR)$@$(ENDCOLOR) 1>&2;
endif

CXX?=c++
WARN=-Wall -W -Wno-missing-field-initializers
STD=-pedantic -std=c++11

OPT?=-O3
CLANG := $(findstring clang,$(shell sh -c '$(CC) --version | head -1'))
ifeq ($(OPT),-O3)
        ifeq (clang,$(CLANG))
                OPTIMIZATION+=-flto
        else
                OPTIMIZATION+=-flto=auto -ffat-lto-objects
        endif
endif

# FASTFLOAT_ALLOWS_LEADING_PLUS allows +inf to be parsed as inf, instead of error. 
CXXFLAGS=$(STD) $(OPT) $(WARN) -fPIC -D FASTFLOAT_ALLOWS_LEADING_PLUS

.PHONY: all clean
all: valkey_strtod.o
clean:
	rm -f *.o || true;

valkey_strtod.o: valkey_strtod.cpp
	$(QUIET_CXX)$(CXX) $(OPT) $(STD) $(WARN) $(CXXFLAGS) -o $@ -c $<
