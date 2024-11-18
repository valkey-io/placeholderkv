/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

#include "../fast_float/fast_float.h"
#include <cerrno>

extern "C"
{
    const char* fast_float_strtod(const char *str, double *value)
    {
        double temp = 0;
        auto answer = fast_float::from_chars(str, str + strlen(str), temp);
        *value = temp;
        if (answer.ec != std::errc()) {
            errno = (answer.ec == std::errc::result_out_of_range) ? ERANGE : EINVAL;
        }
        return answer.ptr;
    }
}
