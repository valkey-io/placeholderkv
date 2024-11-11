#include "fast_float_strtod.h"
#include "assert.h"
#include "string.h"
#include "stdio.h"
#include "errno.h"
#include "math.h" 

void test1()
{
    double value = 0;
    fast_float_strtod("231.2341234", &value);
    assert(value == 231.2341234);
    assert(errno == 0);
    value = 0;  
    fast_float_strtod("+inf", &value);
    assert(isinf(value));
    value = 0;
    fast_float_strtod("-inf", &value);
    assert(isinf(value));
    value = 0;
    fast_float_strtod("inf", &value);
    assert(isinf(value));
    printf("fast_float test succeeded");
}

int main()
{
    test1();
    return 0;
}

