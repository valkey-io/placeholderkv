#ifndef FAST_FLOAT_STRTOD_H
#define FAST_FLOAT_STRTOD_H

#ifdef USE_FAST_FLOAT

#include "errno.h"

/**
 * Converts a null-terminated byte string to a double using the fast_float library.
 *
 * This function provides a C-compatible wrapper around the fast_float library's string-to-double
 * conversion functionality. It aims to offer a faster alternative to the standard strtod function.
 *
 * str: A pointer to the null-terminated byte string to be converted.
 * value: A pointer to the double variable where the function stores converted double value.
 *        On success, the function stores the converted double value. On failure, it stores 0.0 and
 *        stores error code in errno to ERANGE or EINVAL.
 *
 * return value: On success, returns char pointer pointing to '\0' at the end of the string.
 *               On failure, returns char pointer pointing to first invalid character in the string.
 *
 * note: This function uses the fast_float library (https://github.com/fastfloat/fast_float) for
 * the actual conversion, which can be significantly faster than standard library functions.
 *
 * Refer to https://github.com/fastfloat/fast_float for more information on the underlying library.
 */
const char *fast_float_strtod(const char *str, double *value);

static inline const char *valkey_strtod(const char *str, double *value) {
    errno = 0;
    return fast_float_strtod(str, value);
}

#else

#include <stdlib.h>

static inline const char *valkey_strtod(const char *str, double *value) {
    char *endptr;
    *value = strtod(str, &endptr);
    return endptr;
}

#endif

#endif // FAST_FLOAT_STRTOD_H
