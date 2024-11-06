#ifndef FAST_FLOAT_STRTOD_H
#define FAST_FLOAT_STRTOD_H

#ifdef __cplusplus
extern "C"
{
#endif

   /**
    * @brief Converts a null-terminated byte string to a double using the fast_float library.
    *
    * This function provides a C-compatible wrapper around the fast_float library's string-to-double
    * conversion functionality. It aims to offer a faster alternative to the standard strtod function.
    *
    * @param nptr A pointer to the null-terminated byte string to be converted.
    * @param value On success, returns the converted double value.
    *         On failure, returns 0.0 and sets errno to ERANGE (if result is out of range)
    *         or EINVAL (for invalid input).
    *
    * @return If not NULL, a pointer to a pointer to char will be stored with the address
    *         of the first invalid character in nptr. If the function returns successfully,
    *         this will point to the null terminator or any extra characters after the number.
    *
    * @note This function uses the fast_float library (https://github.com/fastfloat/fast_float)
    *       for the actual conversion, which can be significantly faster than standard library functions.
    *
    * @see https://github.com/fastfloat/fast_float for more information on the underlying library.
    */
    const char* fast_float_strtod(const char *nptr, double *value);

#ifdef __cplusplus
}
#endif

#endif // FAST_FLOAT_STRTOD_H
