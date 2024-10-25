#include <ctype.h>
#include <stdio.h>
#include <limits.h>
#include <string.h>
#include <sys/time.h>
#include "test_help.h"

#include "../sds.h"
#include "../sdsalloc.h"

static sds sdsTestTemplateCallback(sds varname, void *arg) {
    UNUSED(arg);
    static const char *_var1 = "variable1";
    static const char *_var2 = "variable2";

    if (!strcmp(varname, _var1))
        return sdsnew("value1");
    else if (!strcmp(varname, _var2))
        return sdsnew("value2");
    else
        return NULL;
}

int test_sds(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    sds x = sdsnew("foo"), y;

    TEST_ASSERT_MESSAGE("Create a string and obtain the length", sdslen(x) == 3 && memcmp(x, "foo\0", 4) == 0);

    sdsfree(x);
    x = sdsnewlen("foo", 2);
    TEST_ASSERT_MESSAGE("Create a string with specified length", sdslen(x) == 2 && memcmp(x, "fo\0", 3) == 0);

    x = sdscat(x, "bar");
    TEST_ASSERT_MESSAGE("Strings concatenation", sdslen(x) == 5 && memcmp(x, "fobar\0", 6) == 0);

    x = sdscpy(x, "a");
    TEST_ASSERT_MESSAGE("sdscpy() against an originally longer string", sdslen(x) == 1 && memcmp(x, "a\0", 2) == 0);

    x = sdscpy(x, "xyzxxxxxxxxxxyyyyyyyyyykkkkkkkkkk");
    TEST_ASSERT_MESSAGE("sdscpy() against an originally shorter string",
                        sdslen(x) == 33 && memcmp(x, "xyzxxxxxxxxxxyyyyyyyyyykkkkkkkkkk\0", 33) == 0);

    sdsfree(x);
    x = sdscatprintf(sdsempty(), "%d", 123);
    TEST_ASSERT_MESSAGE("sdscatprintf() seems working in the base case", sdslen(x) == 3 && memcmp(x, "123\0", 4) == 0);

    sdsfree(x);
    x = sdscatprintf(sdsempty(), "a%cb", 0);
    TEST_ASSERT_MESSAGE("sdscatprintf() seems working with \\0 inside of result", sdslen(x) == 3 && memcmp(x,
                                                                                                           "a\0"
                                                                                                           "b\0",
                                                                                                           4) == 0);

    sdsfree(x);
    char etalon[1024 * 1024];
    for (size_t i = 0; i < sizeof(etalon); i++) {
        etalon[i] = '0';
    }
    x = sdscatprintf(sdsempty(), "%0*d", (int)sizeof(etalon), 0);
    TEST_ASSERT_MESSAGE("sdscatprintf() can print 1MB",
                        sdslen(x) == sizeof(etalon) && memcmp(x, etalon, sizeof(etalon)) == 0);

    sdsfree(x);
    x = sdsnew("--");
    x = sdscatfmt(x, "Hello %s World %I,%I--", "Hi!", LLONG_MIN, LLONG_MAX);
    TEST_ASSERT_MESSAGE("sdscatfmt() seems working in the base case",
                        sdslen(x) == 60 && memcmp(x,
                                                  "--Hello Hi! World -9223372036854775808,"
                                                  "9223372036854775807--",
                                                  60) == 0);

    sdsfree(x);
    x = sdsnew("--");
    x = sdscatfmt(x, "%u,%U--", UINT_MAX, ULLONG_MAX);
    TEST_ASSERT_MESSAGE("sdscatfmt() seems working with unsigned numbers",
                        sdslen(x) == 35 && memcmp(x, "--4294967295,18446744073709551615--", 35) == 0);

    sdsfree(x);
    x = sdsnew(" x ");
    sdstrim(x, " x");
    TEST_ASSERT_MESSAGE("sdstrim() works when all chars match", sdslen(x) == 0);

    sdsfree(x);
    x = sdsnew(" x ");
    sdstrim(x, " ");
    TEST_ASSERT_MESSAGE("sdstrim() works when a single char remains", sdslen(x) == 1 && x[0] == 'x');

    sdsfree(x);
    x = sdsnew("xxciaoyyy");
    sdstrim(x, "xy");
    TEST_ASSERT_MESSAGE("sdstrim() correctly trims characters", sdslen(x) == 4 && memcmp(x, "ciao\0", 5) == 0);

    y = sdsdup(x);
    sdsrange(y, 1, 1);
    TEST_ASSERT_MESSAGE("sdsrange(...,1,1)", sdslen(y) == 1 && memcmp(y, "i\0", 2) == 0);

    sdsfree(y);
    y = sdsdup(x);
    sdsrange(y, 1, -1);
    TEST_ASSERT_MESSAGE("sdsrange(...,1,-1)", sdslen(y) == 3 && memcmp(y, "iao\0", 4) == 0);

    sdsfree(y);
    y = sdsdup(x);
    sdsrange(y, -2, -1);
    TEST_ASSERT_MESSAGE("sdsrange(...,-2,-1)", sdslen(y) == 2 && memcmp(y, "ao\0", 3) == 0);

    sdsfree(y);
    y = sdsdup(x);
    sdsrange(y, 2, 1);
    TEST_ASSERT_MESSAGE("sdsrange(...,2,1)", sdslen(y) == 0 && memcmp(y, "\0", 1) == 0);

    sdsfree(y);
    y = sdsdup(x);
    sdsrange(y, 1, 100);
    TEST_ASSERT_MESSAGE("sdsrange(...,1,100)", sdslen(y) == 3 && memcmp(y, "iao\0", 4) == 0);

    sdsfree(y);
    y = sdsdup(x);
    sdsrange(y, 100, 100);
    TEST_ASSERT_MESSAGE("sdsrange(...,100,100)", sdslen(y) == 0 && memcmp(y, "\0", 1) == 0);

    sdsfree(y);
    y = sdsdup(x);
    sdsrange(y, 4, 6);
    TEST_ASSERT_MESSAGE("sdsrange(...,4,6)", sdslen(y) == 0 && memcmp(y, "\0", 1) == 0);

    sdsfree(y);
    y = sdsdup(x);
    sdsrange(y, 3, 6);
    TEST_ASSERT_MESSAGE("sdsrange(...,3,6)", sdslen(y) == 1 && memcmp(y, "o\0", 2) == 0);

    sdsfree(y);
    sdsfree(x);
    x = sdsnew("foo");
    y = sdsnew("foa");
    TEST_ASSERT_MESSAGE("sdscmp(foo,foa)", sdscmp(x, y) > 0);

    sdsfree(y);
    sdsfree(x);
    x = sdsnew("bar");
    y = sdsnew("bar");
    TEST_ASSERT_MESSAGE("sdscmp(bar,bar)", sdscmp(x, y) == 0);

    sdsfree(y);
    sdsfree(x);
    x = sdsnew("aar");
    y = sdsnew("bar");
    TEST_ASSERT_MESSAGE("sdscmp(bar,bar)", sdscmp(x, y) < 0);

    sdsfree(y);
    sdsfree(x);
    x = sdsnewlen("\a\n\0foo\r", 7);
    y = sdscatrepr(sdsempty(), x, sdslen(x));
    TEST_ASSERT_MESSAGE("sdscatrepr(...data...)", memcmp(y, "\"\\a\\n\\x00foo\\r\"", 15) == 0);

    unsigned int oldfree;
    char *p;
    int i;
    size_t step = 10, j;

    sdsfree(x);
    sdsfree(y);
    x = sdsnew("0");
    TEST_ASSERT_MESSAGE("sdsnew() free/len buffers", sdslen(x) == 1 && sdsavail(x) == 0);

    /* Run the test a few times in order to hit the first two
     * SDS header types. */
    for (i = 0; i < 10; i++) {
        size_t oldlen = sdslen(x);
        x = sdsMakeRoomFor(x, step);
        int type = x[-1] & SDS_TYPE_MASK;

        TEST_ASSERT_MESSAGE("sdsMakeRoomFor() len", sdslen(x) == oldlen);
        if (type != SDS_TYPE_5) {
            TEST_ASSERT_MESSAGE("sdsMakeRoomFor() free", sdsavail(x) >= step);
            oldfree = sdsavail(x);
            UNUSED(oldfree);
        }
        p = x + oldlen;
        for (j = 0; j < step; j++) {
            p[j] = 'A' + j;
        }
        sdsIncrLen(x, step);
    }
    TEST_ASSERT_MESSAGE("sdsMakeRoomFor() content", memcmp("0ABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGH"
                                                           "IJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJ",
                                                           x, 101) == 0);
    TEST_ASSERT_MESSAGE("sdsMakeRoomFor() final length", sdslen(x) == 101);

    sdsfree(x);

    /* Simple template */
    x = sdstemplate("v1={variable1} v2={variable2}", sdsTestTemplateCallback, NULL);
    TEST_ASSERT_MESSAGE("sdstemplate() normal flow", memcmp(x, "v1=value1 v2=value2", 19) == 0);
    sdsfree(x);

    /* Template with callback error */
    x = sdstemplate("v1={variable1} v3={doesnotexist}", sdsTestTemplateCallback, NULL);
    TEST_ASSERT_MESSAGE("sdstemplate() with callback error", x == NULL);

    /* Template with empty var name */
    x = sdstemplate("v1={", sdsTestTemplateCallback, NULL);
    TEST_ASSERT_MESSAGE("sdstemplate() with empty var name", x == NULL);

    /* Template with truncated var name */
    x = sdstemplate("v1={start", sdsTestTemplateCallback, NULL);
    TEST_ASSERT_MESSAGE("sdstemplate() with truncated var name", x == NULL);

    /* Template with quoting */
    x = sdstemplate("v1={{{variable1}} {{} v2={variable2}", sdsTestTemplateCallback, NULL);
    TEST_ASSERT_MESSAGE("sdstemplate() with quoting", memcmp(x, "v1={value1} {} v2=value2", 24) == 0);
    sdsfree(x);

    /* Test sdsResize - extend */
    x = sdsnew("1234567890123456789012345678901234567890");
    x = sdsResize(x, 200, 1);
    TEST_ASSERT_MESSAGE("sdsReszie() expand type", x[-1] == SDS_TYPE_8);
    TEST_ASSERT_MESSAGE("sdsReszie() expand len", sdslen(x) == 40);
    TEST_ASSERT_MESSAGE("sdsReszie() expand strlen", strlen(x) == 40);
    /* Different allocator allocates at least as large as requested size,
     * to confirm the allocator won't waste too much,
     * we add a largest size checker here. */
    TEST_ASSERT_MESSAGE("sdsReszie() expand alloc", sdsalloc(x) >= 200 && sdsalloc(x) < 400);
    /* Test sdsResize - trim free space */
    x = sdsResize(x, 80, 1);
    TEST_ASSERT_MESSAGE("sdsReszie() shrink type", x[-1] == SDS_TYPE_8);
    TEST_ASSERT_MESSAGE("sdsReszie() shrink len", sdslen(x) == 40);
    TEST_ASSERT_MESSAGE("sdsReszie() shrink strlen", strlen(x) == 40);
    TEST_ASSERT_MESSAGE("sdsReszie() shrink alloc", sdsalloc(x) >= 80);
    /* Test sdsResize - crop used space */
    x = sdsResize(x, 30, 1);
    TEST_ASSERT_MESSAGE("sdsReszie() crop type", x[-1] == SDS_TYPE_8);
    TEST_ASSERT_MESSAGE("sdsReszie() crop len", sdslen(x) == 30);
    TEST_ASSERT_MESSAGE("sdsReszie() crop strlen", strlen(x) == 30);
    TEST_ASSERT_MESSAGE("sdsReszie() crop alloc", sdsalloc(x) >= 30);
    /* Test sdsResize - extend to different class */
    x = sdsResize(x, 400, 1);
    TEST_ASSERT_MESSAGE("sdsReszie() expand type", x[-1] == SDS_TYPE_16);
    TEST_ASSERT_MESSAGE("sdsReszie() expand len", sdslen(x) == 30);
    TEST_ASSERT_MESSAGE("sdsReszie() expand strlen", strlen(x) == 30);
    TEST_ASSERT_MESSAGE("sdsReszie() expand alloc", sdsalloc(x) >= 400);
    /* Test sdsResize - shrink to different class */
    x = sdsResize(x, 4, 1);
    TEST_ASSERT_MESSAGE("sdsReszie() crop type", x[-1] == SDS_TYPE_8);
    TEST_ASSERT_MESSAGE("sdsReszie() crop len", sdslen(x) == 4);
    TEST_ASSERT_MESSAGE("sdsReszie() crop strlen", strlen(x) == 4);
    TEST_ASSERT_MESSAGE("sdsReszie() crop alloc", sdsalloc(x) >= 4);
    sdsfree(x);

    return 0;
}

int test_typesAndAllocSize(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    sds x = sdsnewlen(NULL, 31);
    TEST_ASSERT_MESSAGE("len 31 type", (x[-1] & SDS_TYPE_MASK) == SDS_TYPE_5);
    sdsfree(x);

    x = sdsnewlen(NULL, 32);
    TEST_ASSERT_MESSAGE("len 32 type", (x[-1] & SDS_TYPE_MASK) >= SDS_TYPE_8);
    TEST_ASSERT_MESSAGE("len 32 sdsAllocSize", sdsAllocSize(x) == s_malloc_size(sdsAllocPtr(x)));
    sdsfree(x);

    x = sdsnewlen(NULL, 252);
    TEST_ASSERT_MESSAGE("len 252 type", (x[-1] & SDS_TYPE_MASK) >= SDS_TYPE_8);
    TEST_ASSERT_MESSAGE("len 252 sdsAllocSize", sdsAllocSize(x) == s_malloc_size(sdsAllocPtr(x)));
    sdsfree(x);

    x = sdsnewlen(NULL, 253);
    TEST_ASSERT_MESSAGE("len 253 type", (x[-1] & SDS_TYPE_MASK) == SDS_TYPE_16);
    TEST_ASSERT_MESSAGE("len 253 sdsAllocSize", sdsAllocSize(x) == s_malloc_size(sdsAllocPtr(x)));
    sdsfree(x);

    x = sdsnewlen(NULL, 65530);
    TEST_ASSERT_MESSAGE("len 65530 type", (x[-1] & SDS_TYPE_MASK) >= SDS_TYPE_16);
    TEST_ASSERT_MESSAGE("len 65530 sdsAllocSize", sdsAllocSize(x) == s_malloc_size(sdsAllocPtr(x)));
    sdsfree(x);

    x = sdsnewlen(NULL, 65531);
    TEST_ASSERT_MESSAGE("len 65531 type", (x[-1] & SDS_TYPE_MASK) >= SDS_TYPE_32);
    TEST_ASSERT_MESSAGE("len 65531 sdsAllocSize", sdsAllocSize(x) == s_malloc_size(sdsAllocPtr(x)));
    sdsfree(x);

#if (LONG_MAX == LLONG_MAX)
    if (flags & UNIT_TEST_LARGE_MEMORY) {
        x = sdsnewlen(NULL, 4294967286);
        TEST_ASSERT_MESSAGE("len 4294967286 type", (x[-1] & SDS_TYPE_MASK) >= SDS_TYPE_32);
        TEST_ASSERT_MESSAGE("len 4294967286 sdsAllocSize", sdsAllocSize(x) == s_malloc_size(sdsAllocPtr(x)));
        sdsfree(x);

        x = sdsnewlen(NULL, 4294967287);
        TEST_ASSERT_MESSAGE("len 4294967287 type", (x[-1] & SDS_TYPE_MASK) == SDS_TYPE_64);
        TEST_ASSERT_MESSAGE("len 4294967287 sdsAllocSize", sdsAllocSize(x) == s_malloc_size(sdsAllocPtr(x)));
        sdsfree(x);
    }
#endif

    return 0;
}

/* The test verifies that we can adjust SDS types if an allocator returned
 * larger buffer. The maximum length for type SDS_TYPE_X is
 * 2^X - header_size(SDS_TYPE_X) - 1. The maximum value to be stored in alloc
 * field is 2^X - 1. When allocated buffer is larger than
 * 2^X + header_size(SDS_TYPE_X), we "move" to a larger type SDS_TYPE_Y. To be
 * sure SDS_TYPE_Y header fits into 2^X + header_size(SDS_TYPE_X) + 1 bytes, the
 * difference between header sizes must be smaller than
 * header_size(SDS_TYPE_X) + 1.
 * We ignore SDS_TYPE_5 as it doesn't have alloc field. */
int test_sdsHeaderSizes(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    TEST_ASSERT_MESSAGE("can't always adjust SDS_TYPE_8 with SDS_TYPE_16",
                        sizeof(struct sdshdr16) <= 2 * sizeof(struct sdshdr8) + 1);
    TEST_ASSERT_MESSAGE("can't always adjust SDS_TYPE_16 with SDS_TYPE_32",
                        sizeof(struct sdshdr32) <= 2 * sizeof(struct sdshdr16) + 1);
#if (LONG_MAX == LLONG_MAX)
    TEST_ASSERT_MESSAGE("can't always adjust SDS_TYPE_32 with SDS_TYPE_64",
                        sizeof(struct sdshdr64) <= 2 * sizeof(struct sdshdr32) + 1);
#endif

    return 0;
}

int test_sdssplitargs(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    int len;
    sds *sargv;

    sargv = sdssplitargs("Testing one two three", &len);
    TEST_ASSERT(4 == len);
    TEST_ASSERT(!strcmp("Testing", sargv[0]));
    TEST_ASSERT(!strcmp("one", sargv[1]));
    TEST_ASSERT(!strcmp("two", sargv[2]));
    TEST_ASSERT(!strcmp("three", sargv[3]));
    sdsfreesplitres(sargv, len);

    sargv = sdssplitargs("", &len);
    TEST_ASSERT(0 == len);
    TEST_ASSERT(sargv != NULL);
    sdsfreesplitres(sargv, len);

    sargv = sdssplitargs("\"Testing split strings\" \'Another split string\'", &len);
    TEST_ASSERT(2 == len);
    TEST_ASSERT(!strcmp("Testing split strings", sargv[0]));
    TEST_ASSERT(!strcmp("Another split string", sargv[1]));
    sdsfreesplitres(sargv, len);

    sargv = sdssplitargs("\"Hello\" ", &len);
    TEST_ASSERT(1 == len);
    TEST_ASSERT(!strcmp("Hello", sargv[0]));
    sdsfreesplitres(sargv, len);

    char *binary_string = "\"\\x73\\x75\\x70\\x65\\x72\\x20\\x00\\x73\\x65\\x63\\x72\\x65\\x74\\x20\\x70\\x61\\x73\\x73\\x77\\x6f\\x72\\x64\"";
    sargv = sdssplitargs(binary_string, &len);
    TEST_ASSERT(1 == len);
    TEST_ASSERT(22 == sdslen(sargv[0]));
    sdsfreesplitres(sargv, len);

    return 0;
}

/* Included here is the legacy implementation of sdssplitargs to do fuzzing against. */

/* Helper function for sdssplitargs() that returns non zero if 'c'
 * is a valid hex digit. */
static int is_hex_digit(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

/* Helper function for sdssplitargs() that converts a hex digit into an
 * integer from 0 to 15 */
static int hex_digit_to_int(char c) {
    switch (c) {
    case '0': return 0;
    case '1': return 1;
    case '2': return 2;
    case '3': return 3;
    case '4': return 4;
    case '5': return 5;
    case '6': return 6;
    case '7': return 7;
    case '8': return 8;
    case '9': return 9;
    case 'a':
    case 'A': return 10;
    case 'b':
    case 'B': return 11;
    case 'c':
    case 'C': return 12;
    case 'd':
    case 'D': return 13;
    case 'e':
    case 'E': return 14;
    case 'f':
    case 'F': return 15;
    default: return 0;
    }
}

static sds *sdssplitargs_legacy(const char *line, int *argc) {
    const char *p = line;
    char *current = NULL;
    char **vector = NULL;

    *argc = 0;
    while (1) {
        /* skip blanks */
        while (*p && isspace(*p)) p++;
        if (*p) {
            /* get a token */
            int inq = 0;  /* set to 1 if we are in "quotes" */
            int insq = 0; /* set to 1 if we are in 'single quotes' */
            int done = 0;

            if (current == NULL) current = sdsempty();
            while (!done) {
                if (inq) {
                    if (*p == '\\' && *(p + 1) == 'x' && is_hex_digit(*(p + 2)) && is_hex_digit(*(p + 3))) {
                        unsigned char byte;

                        byte = (hex_digit_to_int(*(p + 2)) * 16) + hex_digit_to_int(*(p + 3));
                        current = sdscatlen(current, (char *)&byte, 1);
                        p += 3;
                    } else if (*p == '\\' && *(p + 1)) {
                        char c;

                        p++;
                        switch (*p) {
                        case 'n': c = '\n'; break;
                        case 'r': c = '\r'; break;
                        case 't': c = '\t'; break;
                        case 'b': c = '\b'; break;
                        case 'a': c = '\a'; break;
                        default: c = *p; break;
                        }
                        current = sdscatlen(current, &c, 1);
                    } else if (*p == '"') {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (*(p + 1) && !isspace(*(p + 1))) goto err;
                        done = 1;
                    } else if (!*p) {
                        /* unterminated quotes */
                        goto err;
                    } else {
                        current = sdscatlen(current, p, 1);
                    }
                } else if (insq) {
                    if (*p == '\\' && *(p + 1) == '\'') {
                        p++;
                        current = sdscatlen(current, "'", 1);
                    } else if (*p == '\'') {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (*(p + 1) && !isspace(*(p + 1))) goto err;
                        done = 1;
                    } else if (!*p) {
                        /* unterminated quotes */
                        goto err;
                    } else {
                        current = sdscatlen(current, p, 1);
                    }
                } else {
                    switch (*p) {
                    case ' ':
                    case '\n':
                    case '\r':
                    case '\t':
                    case '\0': done = 1; break;
                    case '"': inq = 1; break;
                    case '\'': insq = 1; break;
                    default: current = sdscatlen(current, p, 1); break;
                    }
                }
                if (*p) p++;
            }
            /* add the token to the vector */
            vector = s_realloc(vector, ((*argc) + 1) * sizeof(char *));
            vector[*argc] = current;
            (*argc)++;
            current = NULL;
        } else {
            /* Even on empty input string return something not NULL. */
            if (vector == NULL) vector = s_malloc(sizeof(void *));
            return vector;
        }
    }

err:
    while ((*argc)--) sdsfree(vector[*argc]);
    s_free(vector);
    if (current) sdsfree(current);
    *argc = 0;
    return NULL;
}

static long long timeInMicroseconds(void) {
    struct timeval tv;

    gettimeofday(&tv, NULL);
    return (((long long)tv.tv_sec) * 1000000) + (tv.tv_usec);
}

#define PRINTABLE_ASCII_START 32
#define PRINTABLE_ASCII_END 126

static char get_random_printable_char(void) {
    return (char)(PRINTABLE_ASCII_START + rand() % (PRINTABLE_ASCII_END - PRINTABLE_ASCII_START + 1));
}

int test_sdssplitargs_benchmark(int argc, char **argv, int flags) {
    if (!(flags & UNIT_TEST_ACCURATE)) return 0;

    UNUSED(argc);
    UNUSED(argv);

    /* Seed the random number generator */
    unsigned int seed = timeInMicroseconds();
    srand(seed);
    TEST_PRINT_INFO("Using random seed: %d", seed);

    const int iterations = 10000;
    const int test_strings = 1000;
    const int test_string_length = 100;

    for (int j = 0; j < test_strings; j++) {
        long i = 0;
        sds random = sdsnew("");
        char bytes[] = "X";
        long long start, elapsed_new, elapsed_old;
        for (int j = 0; j < test_string_length; j++) {
            bytes[0] = get_random_printable_char();
            random = sdscatlen(random, bytes, 1);
        }

        /* Validate the new and legacy implementation return logically the
         * same value. */
        int len1, len2;
        sds *argv1 = sdssplitargs(random, &len1);
        sds *argv2 = sdssplitargs_legacy(random, &len2);

        TEST_ASSERT(len1 == len2);
        for (int j = 0; j < len1; j++) {
            TEST_ASSERT(!sdscmp(argv1[j], argv2[j]));
        }

        int valid_result = argv1 != NULL;
        sdsfreesplitres(argv1, len1);
        sdsfreesplitres(argv2, len2);

        /* Only benchmark valid strings */
        if (!valid_result) continue;

        start = timeInMicroseconds();
        for (; i < iterations; i++) {
            int len;
            sds *argv1 = sdssplitargs(random, &len);
            sdsfreesplitres(argv1, len);
        }
        elapsed_new = timeInMicroseconds() - start;

        start = timeInMicroseconds();
        for (i = 0; i < iterations; i++) {
            int len;
            sds *argv1 = sdssplitargs_legacy(random, &len);
            sdsfreesplitres(argv1, len);
        }
        elapsed_old = timeInMicroseconds() - start;
        TEST_PRINT_INFO("Improvement: %.2f%%, new:%lldus, old:%lldus",
                        (1.0 - (float)elapsed_new / elapsed_old) * 100,
                        elapsed_new,
                        elapsed_old);
        sdsfree(random);
    }

    return 0;
}
