/*
 * Copyright (c) 2024, Valkey contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "fast_float.h"
#include "fast_float_strtod.h"
#include <cerrno>

/**
 * @brief Converts a null-terminated byte string to a double using the fast_float library.
 *
 * This function provides a C-compatible wrapper around the fast_float library's string-to-double
 * conversion functionality. It aims to offer a faster alternative to the standard strtod function.
 *
 * @param nptr A pointer to the null-terminated byte string to be converted.
 * @param endptr If not NULL, a pointer to a pointer to char will be stored with the address
 *               of the first invalid character in nptr. If the function returns successfully,
 *               this will point to the null terminator or any extra characters after the number.
 *
 * @return On success, returns the converted double value.
 *         On failure, returns 0.0 and sets errno to ERANGE (if result is out of range)
 *         or EINVAL (for invalid input).
 *
 * @note This function uses the fast_float library (https://github.com/fastfloat/fast_float)
 *       for the actual conversion, which can be significantly faster than standard library functions.
 *
 * @see https://github.com/fastfloat/fast_float for more information on the underlying library.
 */

extern "C"
{
    const char* fast_float_strtod(const char *nptr, double *value)
    {
        double temp = 0; 
        auto answer = fast_float::from_chars(nptr, nptr + strlen(nptr), temp);
        *value = temp; 
        if (answer.ec != std::errc()) { 
            errno = (answer.ec == std::errc::result_out_of_range) ? ERANGE : EINVAL;
        }
        return const_cast<char *>(answer.ptr);
    }
}
