/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */


#include "../valkey_strtod.h"
#include "errno.h"
#include "math.h"
#include "test_help.h"

int test_valkey_strtod(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    double value = 0;
    errno = 0;
    valkey_strtod("231.2341234", &value);
    TEST_ASSERT(value == 231.2341234);
    TEST_ASSERT(errno == 0);

    value = 0;
    valkey_strtod("+inf", &value);
    TEST_ASSERT(isinf(value));
    TEST_ASSERT(errno == 0);

    value = 0;
    valkey_strtod("-inf", &value);
    TEST_ASSERT(isinf(value));
    TEST_ASSERT(errno == 0);

    value = 0;
    valkey_strtod("inf", &value);
    TEST_ASSERT(isinf(value));
    TEST_ASSERT(errno == 0);

    return 0;
}
