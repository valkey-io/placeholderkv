/*
 * Copyright Valkey Contributors.
 * All rights reserved.
 * SPDX-License-Identifier: BSD 3-Clause
 */

/* Hashset
 * =======
 *
 * This is an implementation of a hash table with cache-line sized buckets. It's
 * designed for speed and low memory overhead. It provides the following
 * features:
 *
 * - Incremental rehashing using two tables.
 *
 * - Stateless iteration using 'scan'.
 *
 * - A hash table contains pointer-sized elements rather than key-value entries.
 *   Using it as a set is straight-forward. Using it as a key-value store
 *   requires combining key and value in an object and inserting this object
 *   into the hash table. A callback for fetching the key from within the object
 *   is provided by the caller when creating the hash table.
 *
 * - The element type, key type, hash function and other properties are
 *   configurable as callbacks in a 'type' structure provided when creating a
 *   hash table.
 *
 * Conventions
 * -----------
 *
 * Functions and types are prefixed by "hashset", macros by "HASHSET". Internal
 * names don't use the prefix. Internal functions are 'static'.
 *
 * Credits
 * -------
 *
 * - The hashset was designed by Viktor Söderqvist.
 * - The bucket chaining is based on an idea by Madelyn Olson.
 * - The cache-line sized bucket is inspired by ideas used in 'Swiss tables'
 *   (Benzaquen, Evlogimenos, Kulukundis, and Perepelitsa et. al.).
 * - The incremental rehashing using two tables and much of the API is based on
 *   the design used in dict, designed by Salvatore Sanfilippo.
 * - The original scan algorithm was designed by Pieter Noordhuis.
 */
#include "hashset.h"
#include "serverassert.h"
#include "zmalloc.h"
#include "mt19937-64.h"
#include "monotonic.h"

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* The default hashing function uses the SipHash implementation in siphash.c. */

uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k);
uint64_t siphash_nocase(const uint8_t *in, const size_t inlen, const uint8_t *k);

/* --- Global variables --- */

static uint8_t hash_function_seed[16];
static hashsetResizePolicy resize_policy = HASHSET_RESIZE_ALLOW;

/* --- Fill factor --- */

/* We use a soft and a hard limit for the minimum and maximum fill factor. The
 * hard limits are used when resizing should be avoided, according to the resize
 * policy. Resizing is typically to be avoided when we have forked child process
 * running. Then, we don't want to move too much memory around, since the fork
 * is using copy-on-write.
 *
 * Even if we resize and start inserting new elements in the new table, we
 * can avoid actively moving elements from the old table to the new table. When
 * the resize policy is AVOID, we perform a step of incremental rehashing only
 * on insertions and not on lookups. */

#define MAX_FILL_PERCENT_SOFT 100
#define MAX_FILL_PERCENT_HARD 500

#define MIN_FILL_PERCENT_SOFT 13
#define MIN_FILL_PERCENT_HARD 3

/* --- Hash function API --- */

/* The seed needs to be 16 bytes. */
void hashsetSetHashFunctionSeed(const uint8_t *seed) {
    memcpy(hash_function_seed, seed, sizeof(hash_function_seed));
}

uint8_t *hashsetGetHashFunctionSeed(void) {
    return hash_function_seed;
}

uint64_t hashsetGenHashFunction(const char *buf, size_t len) {
    return siphash((const uint8_t *)buf, len, hash_function_seed);
}

uint64_t hashsetGenCaseHashFunction(const char *buf, size_t len) {
    return siphash_nocase((const uint8_t *)buf, len, hash_function_seed);
}

/* --- Global resize policy API --- */

/* The global resize policy is one of
 *
 *   - HASHSET_RESIZE_ALLOW: Rehash as required for optimal performance.
 *
 *   - HASHSET_RESIZE_AVOID: Don't rehash and move memory if it can be avoided;
 *     used when there is a fork running and we want to avoid affecting
 *     copy-on-write memory.
 *
 *   - HASHSET_RESIZE_FORBID: Don't rehash at all. Used in a child process which
 *     doesn't add any keys.
 *
 * Incremental rehashing works in the following way: A new table is allocated
 * and elements are incrementally moved from the old to the new table.
 *
 * To avoid affecting copy-on-write, we avoid rehashing when there is a forked
 * child process.
 *
 * We don't completely forbid resizing the table but the fill factor is
 * significantly larger when the resize policy is set to HASHSET_RESIZE_AVOID
 * and we resize with incremental rehashing paused, so new elements are added to
 * the new table and the old elements are rehashed only when the child process
 * is done.
 */
void hashsetSetResizePolicy(hashsetResizePolicy policy) {
    resize_policy = policy;
}

/* --- Hash table layout --- */

#if SIZE_MAX == UINT64_MAX /* 64-bit version */

#define ELEMENTS_PER_BUCKET 7
#define BUCKET_BITS_TYPE uint8_t
#define BITS_NEEDED_TO_STORE_POS_WITHIN_BUCKET 3

/* Selecting the number of buckets.
 *
 * When resizing the table, we want to select an appropriate number of buckets
 * without an expensive division. Division by a power of two is cheap, but any
 * other division is expensive. We pick a fill factor to make division cheap for
 * our choice of ELEMENTS_PER_BUCKET.
 *
 * The number of buckets we want is NUM_ELEMENTS / (ELEMENTS_PER_BUCKET * FILL_FACTOR),
 * rounded up. The fill is the number of elements we have, or want to put, in
 * the table.
 *
 * Instead of the above fraction, we multiply by an integer BUCKET_FACTOR and
 * divide by a power-of-two BUCKET_DIVISOR. This gives us a fill factor of at
 * most MAX_FILL_PERCENT_SOFT, the soft limit for expanding.
 *
 *     NUM_BUCKETS = ceil(NUM_ELEMENTS * BUCKET_FACTOR / BUCKET_DIVISOR)
 *
 * This gives us
 *
 *     FILL_FACTOR = NUM_ELEMENTS / (NUM_BUCKETS * ELEMENTS_PER_BUCKET)
 *                 = 1 / (BUCKET_FACTOR / BUCKET_DIVISOR) / ELEMENTS_PER_BUCKET
 *                 = BUCKET_DIVISOR / BUCKET_FACTOR / ELEMENTS_PER_BUCKET
 */

#define BUCKET_FACTOR 5
#define BUCKET_DIVISOR 32
/* When resizing, we get a fill of at most 91.43% (32 / 5 / 7). */

#define randomSizeT() ((size_t)genrand64_int64())

#elif SIZE_MAX == UINT32_MAX /* 32-bit version */

#define ELEMENTS_PER_BUCKET 12
#define BUCKET_BITS_TYPE uint16_t
#define BITS_NEEDED_TO_STORE_POS_WITHIN_BUCKET 4
#define BUCKET_FACTOR 3
#define BUCKET_DIVISOR 32
/* When resizing, we get a fill of at most 88.89% (32 / 3 / 12). */

#define randomSizeT() ((size_t)random())

#else
#error "Only 64-bit or 32-bit architectures are supported"
#endif /* 64-bit vs 32-bit version */

#ifndef static_assert
#define static_assert _Static_assert
#endif

static_assert(100 * BUCKET_DIVISOR / BUCKET_FACTOR / ELEMENTS_PER_BUCKET <= MAX_FILL_PERCENT_SOFT,
              "Expand must result in a fill below the soft max fill factor");
static_assert(MAX_FILL_PERCENT_SOFT <= MAX_FILL_PERCENT_HARD, "Soft vs hard fill factor");

/* --- Random element --- */

#define FAIR_RANDOM_SAMPLE_SIZE (ELEMENTS_PER_BUCKET * 40)
#define WEAK_RANDOM_SAMPLE_SIZE ELEMENTS_PER_BUCKET

/* --- Types --- */

/* Design
 * ------
 *
 * We use a design with buckets of 64 bytes (one cache line). Each bucket
 * contains metadata and element slots for a fixed number of elements. In a
 * 64-bit system, there are up to 7 elements per bucket. These are unordered and
 * an element can be inserted in any of the free slots. Additionally, the bucket
 * contains metadata for the elements. This includes a few bits of the hash of
 * the key of each element, which are used to rule out false positives when
 * looking up elements.
 *
 * Bucket chaining
 * ---------------
 *
 * Each key hashes to a bucket in the hash table. A bucket has space for 7
 * unordered elements (on 64-bit platforms). If a bucket is full, the last
 * element is replaced by a pointer to a separately allocated child bucket.
 * Child buckets form a bucket chain.
 *
 *           Bucket          Bucket          Bucket
 *     -----+---------------+---------------+---------------+-----
 *      ... | x x x x x x p | x x x x x x x | x x x x x x x | ...
 *     -----+-------------|-+---------------+---------------+-----
 *                        |
 *                        v  Child bucket
 *                      +---------------+
 *                      | x x x x x x p |
 *                      +-------------|-+
 *                                    |
 *                                    v  Child bucket
 *                                  +---------------+
 *                                  | x x x x x x x |
 *                                  +---------------+
 *
 * Within each bucket chain, the elements are unordered. To avoid false
 * positives when looking up an element, a few bits of the hash value is stored
 * in a bucket metadata section in each bucket. The bucket metadata contains a
 * bit that is set when the bucket is full and more elements need to be stored
 * in another bucket. When this bit is set, the last element of the bucket is
 * replaced by pointer to another, separately allocated, child bucket.
 *
 *         +-------------------------------------------------------+
 *         | Meta | Ele- | Ele- | Ele- | Ele- | Ele- | Ele- | Ele- | Bucket
 *         | data | ment | ment | ment | ment | ment | ment | ment | 64 bytes
 *         +-------------------------------------------------------+
 *        /        ` - - . _ _
 *       /                     ` - - . _ _
 *      /                                  ` - - . _
 *     +----------------------------------------------+
 *     | c ppppppp hash hash hash hash hash hash hash |
 *     +----------------------------------------------+
 *      |    |       |
 *      |    |      One byte of hash for each element position in the bucket.
 *      |    |
 *      |   Presence bits. One bit for each element position.
 *      |
 *     Chained? One bit. If set, the last element is a child bucket pointer.
 *
 * Bucket layout, 64-bit version, 7 elements per bucket
 * ----------------------------------------------------
 *
 *     1 bit     7 bits    [1 byte] x 7  [8 bytes] x 7 = 64 bytes
 *     chained   presence  hashes        elements
 *
 *     chained: when set, the last element a pointer to a child bucket
 *     presence: an bit per element slot indicating if an element present or not
 *     hashes: some bits of hash of each element to rule out false positives
 *     elements: the actual elements, typically pointers (pointer-sized)
 *
 * The 32-bit version has 12 elements and 19 unused bits per bucket
 * ----------------------------------------------------------------
 *
 *     1 bit     12 bits   3 bits  [1 byte] x 12  2 bytes  [4 bytes] x 12
 *     chained   presence  unused  hashes         unused   elements
 */

typedef struct hashsetBucket {
    BUCKET_BITS_TYPE chained : 1;
    BUCKET_BITS_TYPE presence : ELEMENTS_PER_BUCKET;
    uint8_t hashes[ELEMENTS_PER_BUCKET];
    void *elements[ELEMENTS_PER_BUCKET];
} bucket;

/* A key property is that the bucket size is one cache line. */
static_assert(sizeof(bucket) == HASHSET_BUCKET_SIZE, "Bucket size mismatch");

struct hashset {
    hashsetType *type;
    ssize_t rehash_idx;        /* -1 = rehashing not in progress. */
    bucket *tables[2];         /* 0 = main table, 1 = rehashing target.  */
    size_t used[2];            /* Number of elements in each table. */
    int8_t bucket_exp[2];      /* Exponent for num buckets (num = 1 << exp). */
    int16_t pause_rehash;      /* Non-zero = rehashing is paused */
    int16_t pause_auto_shrink; /* Non-zero = automatic resizing disallowed. */
    size_t child_buckets[2];   /* Number of allocated child buckets. */
    void *metadata[];
};

/* Struct used for stats functions. */
struct hashsetStats {
    int table_index;                /* 0 or 1 (old or new while rehashing). */
    unsigned long toplevel_buckets; /* Number of buckets in table. */
    unsigned long child_buckets;  /* Number of child buckets. */
    unsigned long size;             /* Capacity of toplevel buckets. */
    unsigned long used;             /* Number of elements in the table. */
    unsigned long max_chain_len;    /* Length of longest bucket chain. */
    unsigned long *clvector;        /* Chain length vector; element i counts
                                     * bucket chains of length i. */
};

/* Struct for sampling elements using scan, used by random key functions. */

typedef struct {
    unsigned size;   /* Size of the elements array. */
    unsigned count;  /* Number of elements already sampled. */
    void **elements; /* Array of sampled elements. */
} scan_samples;

/* --- Internal functions --- */

static bucket *findBucketForInsert(hashset *s, uint64_t hash, int *pos_in_bucket, int *table_index);

static inline void freeElement(hashset *s, void *element) {
    if (s->type->elementDestructor) s->type->elementDestructor(s, element);
}

static inline int compareKeys(hashset *s, const void *key1, const void *key2) {
    if (s->type->keyCompare != NULL) {
        return s->type->keyCompare(s, key1, key2);
    } else {
        return key1 != key2;
    }
}

static inline const void *elementGetKey(hashset *s, const void *element) {
    if (s->type->elementGetKey != NULL) {
        return s->type->elementGetKey(element);
    } else {
        return element;
    }
}

static inline uint64_t hashKey(hashset *s, const void *key) {
    if (s->type->hashFunction != NULL) {
        return s->type->hashFunction(key);
    } else {
        return hashsetGenHashFunction((const char *)&key, sizeof(key));
    }
}

static inline uint64_t hashElement(hashset *s, const void *element) {
    return hashKey(s, elementGetKey(s, element));
}


/* For the hash bits stored in the bucket, we use the highest bits of the hash
 * value, since these are not used for selecting the bucket. */
static inline uint8_t highBits(uint64_t hash) {
    return hash >> (CHAR_BIT * 7);
}

static inline int bucketIsFull(bucket *b) {
    int num_positions = ELEMENTS_PER_BUCKET - (b->chained ? 1 : 0);
    return b->presence == (1 << num_positions) - 1;
}

/* Returns non-zero if the position within the bucket is occupied. */
static inline int isPositionFilled(bucket *b, int position) {
    return b->presence & (1 << position);
}
static void resetTable(hashset *s, int table_idx) {
    s->tables[table_idx] = NULL;
    s->used[table_idx] = 0;
    s->bucket_exp[table_idx] = -1;
    s->child_buckets[table_idx] = 0;
}

/* Number of top-level buckets. */
static inline size_t numBuckets(int exp) {
    return exp == -1 ? 0 : (size_t)1 << exp;
}

/* Bitmask for masking the hash value to get bucket index. */
static inline size_t expToMask(int exp) {
    return exp == -1 ? 0 : numBuckets(exp) - 1;
}

/* Returns the 'exp', where num_buckets = 1 << exp. The number of
 * buckets is a power of two. */
static signed char nextBucketExp(size_t min_capacity) {
    if (min_capacity == 0) return -1;
    /* ceil(x / y) = floor((x - 1) / y) + 1 */
    size_t min_buckets = (min_capacity * BUCKET_FACTOR - 1) / BUCKET_DIVISOR + 1;
    if (min_buckets >= SIZE_MAX / 2) return CHAR_BIT * sizeof(size_t) - 1;
    if (min_buckets == 1) return 0;
    return CHAR_BIT * sizeof(size_t) - __builtin_clzl(min_buckets - 1);
}

/* Swaps the tables and frees the old table. */
static void rehashingCompleted(hashset *s) {
    if (s->type->rehashingCompleted) s->type->rehashingCompleted(s);
    if (s->tables[0]) zfree(s->tables[0]);
    s->bucket_exp[0] = s->bucket_exp[1];
    s->tables[0] = s->tables[1];
    s->used[0] = s->used[1];
    s->child_buckets[0] = s->child_buckets[1];
    resetTable(s, 1);
    s->rehash_idx = -1;
}

/* Reverse bits, adapted to use bswap, from
 * https://graphics.stanford.edu/~seander/bithacks.html#ReverseParallel */
static size_t rev(size_t v) {
#if SIZE_MAX == UINT64_MAX
    /* Swap odd and even bits. */
    v = ((v >> 1) & 0x5555555555555555) | ((v & 0x5555555555555555) << 1);
    /* Swap consecutive pairs. */
    v = ((v >> 2) & 0x3333333333333333) | ((v & 0x3333333333333333) << 2);
    /* Swap nibbles. */
    v = ((v >> 4) & 0x0F0F0F0F0F0F0F0F) | ((v & 0x0F0F0F0F0F0F0F0F) << 4);
    /* Reverse bytes. */
    v = __builtin_bswap64(v);
#else
    /* 32-bit version. */
    v = ((v >> 1) & 0x55555555) | ((v & 0x55555555) << 1);
    v = ((v >> 2) & 0x33333333) | ((v & 0x33333333) << 2);
    v = ((v >> 4) & 0x0F0F0F0F) | ((v & 0x0F0F0F0F) << 4);
    v = __builtin_bswap32(v);
#endif
    return v;
}

/* Advances a scan cursor to the next value. It increments the reverse bit
 * representation of the masked bits of v. This algorithm was invented by Pieter
 * Noordhuis. */
size_t nextCursor(size_t v, size_t mask) {
    v |= ~mask; /* Set the unmasked (high) bits. */
    v = rev(v); /* Reverse. The unmasked bits are now the low bits. */
    v++;        /* Increment the reversed cursor, flipping the unmasked bits to
                 * 0 and increments the masked bits. */
    v = rev(v); /* Reverse the bits back to normal. */
    return v;
}

/* Returns the next bucket in a bucket chain, or NULL if there's no next. */
static bucket *bucketNext(bucket *b) {
    return b->chained ? b->elements[ELEMENTS_PER_BUCKET - 1] : NULL;
}

/* Attempts to defrag bucket 'b' using the defrag callback function. If the
 * defrag callback function returns a pointer to a new allocation, this pointer
 * is returned and the 'prev' bucket is updated to point to the new allocation.
 * Otherwise, the 'b' pointer is returned. */
static bucket *bucketDefrag(bucket *prev, bucket *b, void *(*defragfn)(void *)) {
    bucket *reallocated = defragfn(b);
    if (reallocated == NULL) return b;
    prev->elements[ELEMENTS_PER_BUCKET - 1] = reallocated;
    return reallocated;
}

/* Rehashes one bucket. */
static void rehashBucket(hashset *s, bucket *b) {
    int num_positions = ELEMENTS_PER_BUCKET - (b->chained ? 1 : 0);
    int pos;
    for (pos = 0; pos < num_positions; pos++) {
        if (!isPositionFilled(b, pos)) continue; /* empty */
        void *element = b->elements[pos];
        uint8_t h2 = b->hashes[pos];
        /* Insert into table 1. */
        uint64_t hash;
        /* When shrinking, it's possible to avoid computing the hash. We can
         * just use idx has the hash. */
        if (s->bucket_exp[1] < s->bucket_exp[0]) {
            hash = s->rehash_idx;
        } else {
            hash = hashElement(s, element);
        }
        int pos_in_dst_bucket;
        bucket *dst = findBucketForInsert(s, hash, &pos_in_dst_bucket, NULL);
        dst->elements[pos_in_dst_bucket] = element;
        dst->hashes[pos_in_dst_bucket] = h2;
        dst->presence |= (1 << pos_in_dst_bucket);
        s->used[0]--;
        s->used[1]++;
    }
    /* Mark the source bucket as empty. */
    b->presence = 0;
}

static void rehashStep(hashset *s) {
    assert(hashsetIsRehashing(s));
    size_t idx = s->rehash_idx;
    bucket *b = &s->tables[0][idx];
    rehashBucket(s, b);
    if (b->chained) {
        /* Rehash and free child buckets. */
        bucket *next = bucketNext(b);
        b->chained = 0;
        b = next;
        while (b != NULL) {
            rehashBucket(s, b);
            next = bucketNext(b);
            zfree(b);
            s->child_buckets[0]--;
            b = next;
        }
    }

    /* Advance to the next bucket. */
    s->rehash_idx++;
    if ((size_t)s->rehash_idx >= numBuckets(s->bucket_exp[0])) {
        if ((size_t)s->rehash_idx > numBuckets(s->bucket_exp[0])) {
            printf("rehash_idx > numBuckets! %zd > %zu\n",
                   s->rehash_idx, numBuckets(s->bucket_exp[0]));
        }
        rehashingCompleted(s);
    }
}

/* Called internally on lookup and other reads to the table. */
static inline void rehashStepOnReadIfNeeded(hashset *s) {
    if (!hashsetIsRehashing(s) || s->pause_rehash) return;
    if (resize_policy != HASHSET_RESIZE_ALLOW) return;
    rehashStep(s);
}

/* When inserting or deleting, we first do a find (read) and rehash one step if
 * resize policy is set to ALLOW, so here we only do it if resize policy is
 * AVOID. The reason for doing it on insert and delete is to ensure that we
 * finish rehashing before we need to resize the table again. */
static inline void rehashStepOnWriteIfNeeded(hashset *s) {
    if (!hashsetIsRehashing(s) || s->pause_rehash) return;
    if (resize_policy != HASHSET_RESIZE_AVOID) return;
    rehashStep(s);
}

/* Allocates a new table and initiates incremental rehashing if necessary.
 * Returns 1 on resize (success), 0 on no resize (failure). If 0 is returned and
 * 'malloc_failed' is provided, it is set to 1 if allocation failed. If
 * 'malloc_failed' is not provided, an allocation failure triggers a panic. */
static int resize(hashset *s, size_t min_capacity, int *malloc_failed) {
    if (malloc_failed) *malloc_failed = 0;

    /* Adjust minimum size. We don't resize to zero currently. */
    if (min_capacity == 0) min_capacity = 1;

    /* Size of new table. */
    signed char exp = nextBucketExp(min_capacity);
    size_t num_buckets = numBuckets(exp);
    size_t new_capacity = num_buckets * ELEMENTS_PER_BUCKET;
    if (new_capacity < min_capacity || num_buckets * sizeof(bucket) < num_buckets) {
        /* Overflow */
        return 0;
    }

    signed char old_exp = s->bucket_exp[hashsetIsRehashing(s) ? 1 : 0];
    size_t alloc_size = num_buckets * sizeof(bucket);
    if (exp == old_exp) {
        /* Can't resize to same size. */
        return 0;
    }

    if (s->type->resizeAllowed) {
        double fill_factor = (double)min_capacity / ((double)numBuckets(old_exp) * ELEMENTS_PER_BUCKET);
        if (fill_factor * 100 < MAX_FILL_PERCENT_HARD && !s->type->resizeAllowed(alloc_size, fill_factor)) {
            /* Resize callback says no. */
            return 0;
        }
    }

    /* We can't resize if rehashing is already ongoing. Fast-forward ongoing
     * rehashing before we continue. This can happen only in exceptional
     * scenarios, such as when many insertions are made while rehashing is
     * paused. */
    if (hashsetIsRehashing(s)) {
        if (hashsetIsRehashingPaused(s)) return 0;
        while (hashsetIsRehashing(s)) {
            rehashStep(s);
        }
    }

    /* Allocate the new hash table. */
    bucket *new_table;
    if (malloc_failed) {
        new_table = ztrycalloc(alloc_size);
        if (new_table == NULL) {
            *malloc_failed = 1;
            return 0;
        }
    } else {
        new_table = zcalloc(alloc_size);
    }
    s->bucket_exp[1] = exp;
    s->tables[1] = new_table;
    s->used[1] = 0;
    s->rehash_idx = 0;
    if (s->type->rehashingStarted) s->type->rehashingStarted(s);

    /* If the old table was empty, the rehashing is completed immediately. */
    if (s->tables[0] == NULL || s->used[0] == 0) {
        rehashingCompleted(s);
    } else if (s->type->instant_rehashing) {
        while (hashsetIsRehashing(s)) {
            rehashStep(s);
        }
    }
    return 1;
}

/* Returns 1 if the table is expanded, 0 if not expanded. If 0 is returned and
 * 'malloc_failed' is proveded, it is set to 1 if malloc failed and 0
 * otherwise. */
static int expand(hashset *s, size_t size, int *malloc_failed) {
    if (size < hashsetSize(s)) {
        return 0;
    }
    return resize(s, size, malloc_failed);
}

/* Finds an element matching the key. If a match is found, returns a pointer to
 * the bucket containing the matching element and points 'pos_in_bucket' to the
 * index within the bucket. Returns NULL if no matching element was found.
 *
 * If 'table_index' is provided, it is set to the index of the table (0 or 1)
 * the returned bucket belongs to. */
static bucket *findBucket(hashset *s, uint64_t hash, const void *key, int *pos_in_bucket, int *table_index) {
    if (hashsetSize(s) == 0) return 0;
    uint8_t h2 = highBits(hash);
    int table;

    /* Do some incremental rehashing. */
    rehashStepOnReadIfNeeded(s);

    for (table = 0; table <= 1; table++) {
        if (s->used[table] == 0) continue;
        size_t mask = expToMask(s->bucket_exp[table]);
        size_t bucket_idx = hash & mask;
        /* Skip already rehashed buckets. */
        if (table == 0 && s->rehash_idx >= 0 && bucket_idx < (size_t)s->rehash_idx) {
            continue;
        }
        bucket *b = &s->tables[table][bucket_idx];
        do {
            /* Find candidate elements with presence flag set and matching h2 hash. */
            int num_positions = ELEMENTS_PER_BUCKET - (b->chained ? 1 : 0);
            for (int pos = 0; pos < num_positions; pos++) {
                if (isPositionFilled(b, pos) && b->hashes[pos] == h2) {
                    /* It's a candidate. */
                    void *element = b->elements[pos];
                    const void *elem_key = elementGetKey(s, element);
                    if (compareKeys(s, key, elem_key) == 0) {
                        /* It's a match. */
                        assert(pos_in_bucket != NULL);
                        *pos_in_bucket = pos;
                        if (table_index) *table_index = table;
                        return b;
                    }
                }
            }
            b = bucketNext(b);
        } while (b != NULL);
    }
    return NULL;
}

/* Move an element from one bucket to another. */
static void moveElement(bucket *bucket_to, int pos_to, bucket *bucket_from, int pos_from) {
    assert(!isPositionFilled(bucket_to, pos_to));
    assert(isPositionFilled(bucket_from, pos_from));
    bucket_to->elements[pos_to] = bucket_from->elements[pos_from];
    bucket_to->hashes[pos_to] = bucket_from->hashes[pos_from];
    bucket_to->presence |= (1 << pos_to);
    bucket_from->presence &= ~(1 << pos_from);
}

/* Converts a full bucket b to a chained bucket and adds a new child bucket. The
 * new child bucket is returned. */
static void bucketConvertToChained(bucket *b) {
    assert(!b->chained);
    /* We'll move the last element from the bucket to the new child bucket. */
    int pos = ELEMENTS_PER_BUCKET - 1;
    assert(isPositionFilled(b, pos));
    bucket *child = zcalloc(sizeof(bucket));
    moveElement(child, 0, b, pos);
    b->chained = 1;
    b->elements[pos] = child;
}

/* Converts a bucket with a next-bucket pointer to one without one. */
static void bucketConvertToUnchained(bucket *b) {
    assert(b->chained);
    b->chained = 0;
    assert(!isPositionFilled(b, ELEMENTS_PER_BUCKET - 1));
}

/* If the last bucket is empty, free it. The before-last bucket is converted
 * back to an "unchained" bucket, becoming the new last bucket in the chain. If
 * there's only one element left in the last bucket, it's moved to the
 * before-last bucket's last position, to take the place of the next-bucket
 * link.
 *
 * This function needs the penultimate 'before_last' bucket in the chain, to be
 * able to update it when the last bucket is freed. */
static void pruneLastBucket(hashset *s, bucket *before_last, bucket *last, int table_index) {
    assert(before_last->chained);
    assert(!last->chained);
    assert(last->presence == 0 || __builtin_popcount(last->presence) == 1);
    bucketConvertToUnchained(before_last);
    if (last->presence != 0) {
        /* Move the last remaining element to the new last position in the
         * before-last bucket. */
        int pos_in_last = __builtin_ctz(last->presence);
        moveElement(before_last, ELEMENTS_PER_BUCKET - 1, last, pos_in_last);
    }
    zfree(last);
    s->child_buckets[table_index]--;
}

/* After removing an element in a bucket with children, we can fill the hole
 * with an element from the end of the bucket chain and potentially free the
 * last bucket in the chain. */
static void fillBucketHole(hashset *s, bucket *b, int pos_in_bucket, int table_index) {
    assert(b->chained && !isPositionFilled(b, pos_in_bucket));
    /* Find the last bucket */
    bucket *before_last = b;
    bucket *last = bucketNext(b);
    while (last->chained) {
        before_last = last;
        last = bucketNext(last);
    }
    /* Unless the last bucket is empty, find an element in the last bucket and
     * move it to the hole in b. */
    if (last->presence != 0) {
        int pos_in_last = __builtin_ctz(last->presence);
        assert(pos_in_last < ELEMENTS_PER_BUCKET && isPositionFilled(last, pos_in_last));
        moveElement(b, pos_in_bucket, last, pos_in_last);
    }
    /* Free the last bucket if it becomes empty. */
    if (last->presence == 0 || __builtin_popcount(last->presence) == 1) {
        pruneLastBucket(s, before_last, last, table_index);
    }
}

/* When elements are deleted while rehashing is paused, they leave empty holes
 * in the buckets. This functions attempts to fill the holes by moving elements
 * from the end of the bucket chain to fill the holes and free any empty
 * buckets in the end of the chain. */
static void compactBucketChain(hashset *s, size_t bucket_index, int table_index) {
    bucket *b = &s->tables[table_index][bucket_index];
    while (b->chained) {
        bucket *next = bucketNext(b);
        if (next->chained && next->presence == 0) {
            /* Empty bucket in the middle of the chain. Remove it from the chain. */
            bucket *next_next = bucketNext(next);
            b->elements[ELEMENTS_PER_BUCKET - 1] = next_next;
            zfree(next);
            s->child_buckets[table_index]--;
            continue;
        }

        if (!next->chained && (next->presence == 0 || __builtin_popcount(next->presence) == 1)) {
            /* Next is the last bucket and it's empty or has only one element.
             * Delete it and turn b into an "unchained" bucket. */
            pruneLastBucket(s, b, next, table_index);
            return;
        }

        if (__builtin_popcount(b->presence) == ELEMENTS_PER_BUCKET - 1) {
            /* Fill the holes in the bucket. */
            for (int pos = 0; pos < ELEMENTS_PER_BUCKET - 1; pos++) {
                if (!isPositionFilled(b, pos)) {
                    fillBucketHole(s, b, pos, table_index);
                    if (!b->chained) return;
                }
            }
        }

        /* Bucket is full. Move forward to next bucket. */
        b = next;
    }
}

/* Find an empty position in the table for inserting an element with the given hash. */
static bucket *findBucketForInsert(hashset *s, uint64_t hash, int *pos_in_bucket, int *table_index) {
    int table = hashsetIsRehashing(s) ? 1 : 0;
    assert(s->tables[table]);
    size_t mask = expToMask(s->bucket_exp[table]);
    size_t bucket_idx = hash & mask;
    bucket *b = &s->tables[table][bucket_idx];
    /* Find bucket that's not full, or create one. */
    while (bucketIsFull(b)) {
        if (!b->chained) {
            bucketConvertToChained(b);
            s->child_buckets[table]++;
        }
        b = bucketNext(b);
    }
    /* Find a free slot in the bucket. There must be at least one. */
    int pos;
    for (pos = 0; pos < ELEMENTS_PER_BUCKET; pos++) {
        if (!isPositionFilled(b, pos)) break;
    }
    assert(pos < ELEMENTS_PER_BUCKET);
    assert(pos_in_bucket != NULL);
    *pos_in_bucket = pos;
    if (table_index) *table_index = table;
    return b;
}

/* Helper to insert an element. Doesn't check if an element with a matching key
 * already exists. This must be ensured by the caller. */
static void insert(hashset *s, uint64_t hash, void *element) {
    hashsetExpandIfNeeded(s);
    rehashStepOnWriteIfNeeded(s);
    int pos_in_bucket;
    int table_index;
    bucket *b = findBucketForInsert(s, hash, &pos_in_bucket, &table_index);
    b->elements[pos_in_bucket] = element;
    b->presence |= (1 << pos_in_bucket);
    b->hashes[pos_in_bucket] = highBits(hash);
    s->used[table_index]++;
}

/* A 63-bit fingerprint of some of the state of the hash table. */
static uint64_t hashsetFingerprint(hashset *s) {
    uint64_t integers[6], hash = 0;
    integers[0] = (uintptr_t)s->tables[0];
    integers[1] = s->bucket_exp[0];
    integers[2] = s->used[0];
    integers[3] = (uintptr_t)s->tables[1];
    integers[4] = s->bucket_exp[1];
    integers[5] = s->used[1];

    /* Result = hash(hash(hash(int1)+int2)+int3) */
    for (int j = 0; j < 6; j++) {
        hash += integers[j];
        /* Tomas Wang's 64 bit integer hash. */
        hash = (~hash) + (hash << 21); /* hash = (hash << 21) - hash - 1; */
        hash = hash ^ (hash >> 24);
        hash = (hash + (hash << 3)) + (hash << 8); /* hash * 265 */
        hash = hash ^ (hash >> 14);
        hash = (hash + (hash << 2)) + (hash << 4); /* hash * 21 */
        hash = hash ^ (hash >> 28);
        hash = hash + (hash << 31);
    }

    /* Clear the highest bit. We only want 63 bits. */
    hash &= 0x7fffffffffffffff;
    return hash;
}

/* Scan callback function used by hashsetGetSomeElements() for sampling elements
 * using scan. */
static void sampleElementsScanFn(void *privdata, void *element) {
    scan_samples *samples = privdata;
    if (samples->count < samples->size) {
        samples->elements[samples->count++] = element;
    }
}

/* --- API functions --- */

/* Allocates and initializes a new hashtable specified by the given type. */
hashset *hashsetCreate(hashsetType *type) {
    size_t metasize = type->getMetadataSize ? type->getMetadataSize() : 0;
    hashset *s = zmalloc(sizeof(*s) + metasize);
    if (metasize > 0) {
        memset(&s->metadata, 0, metasize);
    }
    s->type = type;
    s->rehash_idx = -1;
    s->pause_rehash = 0;
    s->pause_auto_shrink = 0;
    resetTable(s, 0);
    resetTable(s, 1);
    return s;
}

/* Deletes all the elements. If a callback is provided, it is called from time
 * to time to indicate progress. */
void hashsetEmpty(hashset *s, void(callback)(hashset *)) {
    if (hashsetIsRehashing(s)) {
        /* Pretend rehashing completed. */
        if (s->type->rehashingCompleted) s->type->rehashingCompleted(s);
        s->rehash_idx = -1;
    }
    for (int table_index = 0; table_index <= 1; table_index++) {
        if (s->bucket_exp[table_index] < 0) {
            continue;
        }
        if (s->used[table_index] > 0) {
            for (size_t idx = 0; idx < numBuckets(s->bucket_exp[table_index]); idx++) {
                if (callback && (idx & 65535) == 0) callback(s);
                bucket *b = &s->tables[table_index][idx];
                do {
                    /* Call the destructor with each element. */
                    if (s->type->elementDestructor != NULL && b->presence != 0) {
                        for (int pos = 0; pos < ELEMENTS_PER_BUCKET; pos++) {
                            if (isPositionFilled(b, pos)) {
                                s->type->elementDestructor(s, b->elements[pos]);
                            }
                        }
                    }
                    bucket *next = bucketNext(b);

                    /* Free allocated bucket. */
                    if (b != &s->tables[table_index][idx]) {
                        zfree(b);
                    }
                    b = next;
                } while (b != NULL);
            }
        }
        zfree(s->tables[table_index]);
        resetTable(s, table_index);
    }
}

/* Deletes all the elements and frees the table. */
void hashsetRelease(hashset *s) {
    hashsetEmpty(s, NULL);
    zfree(s);
}

/* Returns the type of the hashtable. */
hashsetType *hashsetGetType(hashset *s) {
    return s->type;
}

/* Returns a pointer to the table's metadata (userdata) section. */
void *hashsetMetadata(hashset *s) {
    return &s->metadata;
}

/* Returns the number of elements stored. */
size_t hashsetSize(hashset *s) {
    return s->used[0] + s->used[1];
}

/* Returns the number of buckets in the hash table itself. */
size_t hashsetBuckets(hashset *s) {
    return numBuckets(s->bucket_exp[0]) + numBuckets(s->bucket_exp[1]);
}

/* Returns the number of buckets that have a child bucket. Equivalently, the
 * number of allocated buckets, outside of the hash table itself. */
size_t hashsetChainedBuckets(hashset *s, int table) {
    return s->child_buckets[table];
}

/* Returns the size of the hashset structures, in bytes (not including the sizes
 * of the elements, if the elements are pointers to allocated objects). */
size_t hashsetMemUsage(hashset *s) {
    size_t num_buckets = numBuckets(s->bucket_exp[0]) + numBuckets(s->bucket_exp[1]);
    num_buckets = s->child_buckets[0] + s->child_buckets[1];
    size_t metasize = s->type->getMetadataSize ? s->type->getMetadataSize() : 0;
    return sizeof(hashset) + metasize + sizeof(bucket) * num_buckets;
}

/* Pauses automatic shrinking. This can be called before deleting a lot of
 * elements, to prevent automatic shrinking from being triggered multiple times.
 * Call hashtableResumeAutoShrink afterwards to restore automatic shrinking. */
void hashsetPauseAutoShrink(hashset *s) {
    s->pause_auto_shrink++;
}

/* Re-enables automatic shrinking, after it has been paused. If you have deleted
 * many elements while automatic shrinking was paused, you may want to call
 * hashsetShrinkIfNeeded. */
void hashsetResumeAutoShrink(hashset *s) {
    s->pause_auto_shrink--;
    if (s->pause_auto_shrink == 0) {
        hashsetShrinkIfNeeded(s);
    }
}

/* Pauses incremental rehashing. When rehashing is paused, bucket chains are not
 * automatically compacted when elements are deleted. Doing so may leave empty
 * spaces, "holes", in the bucket chains, which wastes memory. */
void hashsetPauseRehashing(hashset *s) {
    s->pause_rehash++;
}

/* Resumes incremental rehashing, after pausing it. */
void hashsetResumeRehashing(hashset *s) {
    s->pause_rehash--;
}

/* Returns 1 if incremental rehashing is paused, 0 if it isn't. */
int hashsetIsRehashingPaused(hashset *s) {
    return s->pause_rehash > 0;
}

/* Returns 1 if incremental rehashing is in progress, 0 otherwise. */
int hashsetIsRehashing(hashset *s) {
    return s->rehash_idx != -1;
}

/* Provides the number of buckets in the old and new tables during rehashing.
 * To get the sizes in bytes, multiply by HASHTAB_BUCKET_SIZE. This function can
 * only be used when rehashing is in progress, and from the rehashingStarted and
 * rehashingCompleted callbacks. */
void hashsetRehashingInfo(hashset *s, size_t *from_size, size_t *to_size) {
    assert(hashsetIsRehashing(s));
    *from_size = numBuckets(s->bucket_exp[0]);
    *to_size = numBuckets(s->bucket_exp[1]);
}

int hashsetRehashMicroseconds(hashset *s, uint64_t us) {
    if (s->pause_rehash > 0) return 0;
    if (resize_policy != HASHSET_RESIZE_ALLOW) return 0;

    monotime timer;
    elapsedStart(&timer);
    int rehashes = 0;

    while (hashsetIsRehashing(s)) {
        rehashStep(s);
        rehashes++;
        if (rehashes % 128 == 0 && elapsedUs(timer) >= us) break;
    }
    return rehashes;
}

/* Return 1 if expand was performed; 0 otherwise. */
int hashsetExpand(hashset *s, size_t size) {
    return expand(s, size, NULL);
}

/* Returns 1 if expand was performed or if expand is not needed. Returns 0 if
 * expand failed due to memory allocation failure. */
int hashsetTryExpand(hashset *s, size_t size) {
    int malloc_failed = 0;
    return expand(s, size, &malloc_failed) || !malloc_failed;
}

/* Expanding is done automatically on insertion, but less eagerly if resize
 * policy is set to AVOID or FORBID. After restoring resize policy to ALLOW, you
 * may want to call hashsetExpandIfNeeded. Returns 1 if expanding, 0 if not
 * expanding. */
int hashsetExpandIfNeeded(hashset *s) {
    size_t min_capacity = s->used[0] + s->used[1] + 1;
    size_t num_buckets = numBuckets(s->bucket_exp[hashsetIsRehashing(s) ? 1 : 0]);
    size_t current_capacity = num_buckets * ELEMENTS_PER_BUCKET;
    unsigned max_fill_percent = resize_policy == HASHSET_RESIZE_AVOID ? MAX_FILL_PERCENT_HARD : MAX_FILL_PERCENT_SOFT;
    if (min_capacity * 100 <= current_capacity * max_fill_percent) {
        return 0;
    }
    return resize(s, min_capacity, NULL);
}

/* Shrinking is done automatically on deletion, but less eagerly if resize
 * policy is set to AVOID and not at all if set to FORBID. After restoring
 * resize policy to ALLOW, you may want to call hashsetShrinkIfNeeded. */
int hashsetShrinkIfNeeded(hashset *s) {
    /* Don't shrink if rehashing is already in progress. */
    if (hashsetIsRehashing(s) || resize_policy == HASHSET_RESIZE_FORBID) {
        return 0;
    }
    size_t current_capacity = numBuckets(s->bucket_exp[0]) * ELEMENTS_PER_BUCKET;
    unsigned min_fill_percent = resize_policy == HASHSET_RESIZE_AVOID ? MIN_FILL_PERCENT_HARD : MIN_FILL_PERCENT_SOFT;
    if (s->used[0] * 100 > current_capacity * min_fill_percent) {
        return 0;
    }
    return resize(s, s->used[0], NULL);
}

/* Defragment the internal allocations of the hashset by reallocating them. The
 * provided defragfn callback should either return NULL (if reallocation is not
 * necessary) or reallocate the memory like realloc() would do.
 *
 * Returns NULL if the hashset's top-level struct hasn't been reallocated.
 * Returns non-NULL if the top-level allocation has been allocated and thus
 * making the 's' pointer invalid. */
hashset *hashsetDefragInternals(hashset *s, void *(*defragfn)(void *)) {
    /* The hashset struct */
    hashset *s1 = defragfn(s);
    if (s1 != NULL) s = s1;
    /* The tables */
    for (int i = 0; i <= 1; i++) {
        if (s->tables[i] == NULL) continue;
        void *table = defragfn(s->tables[i]);
        if (table != NULL) s->tables[i] = table;
    }
    return s1;
}

/* Returns 1 if an element was found matching the key. Also points *found to it,
 * if found is provided. Returns 0 if no matching element was found. */
int hashsetFind(hashset *s, const void *key, void **found) {
    if (hashsetSize(s) == 0) return 0;
    uint64_t hash = hashKey(s, key);
    int pos_in_bucket = 0;
    bucket *b = findBucket(s, hash, key, &pos_in_bucket, NULL);
    if (b) {
        if (found) *found = b->elements[pos_in_bucket];
        return 1;
    } else {
        return 0;
    }
}

/* Returns a pointer to where an element is stored within the hash table, or
 * NULL if not found. To get the element, dereference the returned pointer. The
 * pointer can be used to replace the element with an equivalent element (same
 * key, same hash value), but note that the pointer may be invalidated by future
 * accesses to the hash table due to incermental rehashing, so use with care. */
void **hashsetFindRef(hashset *s, const void *key) {
    if (hashsetSize(s) == 0) return NULL;
    uint64_t hash = hashKey(s, key);
    int pos_in_bucket = 0;
    bucket *b = findBucket(s, hash, key, &pos_in_bucket, NULL);
    return b ? &b->elements[pos_in_bucket] : NULL;
}

/* Adds an element. Returns 1 on success. Returns 0 if there was already an element
 * with the same key. */
int hashsetAdd(hashset *s, void *element) {
    return hashsetAddOrFind(s, element, NULL);
}

/* Adds an element and returns 1 on success. Returns 0 if there was already an
 * element with the same key and, if an 'existing' pointer is provided, it is
 * pointed to the existing element. */
int hashsetAddOrFind(hashset *s, void *element, void **existing) {
    const void *key = elementGetKey(s, element);
    uint64_t hash = hashKey(s, key);
    int pos_in_bucket = 0;
    bucket *b = findBucket(s, hash, key, &pos_in_bucket, NULL);
    if (b != NULL) {
        if (existing) *existing = b->elements[pos_in_bucket];
        return 0;
    } else {
        insert(s, hash, element);
        return 1;
    }
}

/* Finds a position within the hashset where an element with the
 * given key should be inserted using hashsetInsertAtPosition. This is the first
 * phase in a two-phase insert operation and it can be used if you want to avoid
 * creating an element before you know if it already exists in the table or not,
 * and without a separate lookup to the table.
 *
 * The function returns 1 if a position was found where an element with the
 * given key can be inserted. The position is stored in provided 'position'
 * argument, which can be stack-allocated. This position should then be used in
 * a call to hashsetInsertAtPosition.
 *
 * If the function returns 0, it means that an an element with the given key
 * already exists in the table. If an 'existing' pointer is provided, it is
 * pointed to the existing element with the matching key.
 *
 * Example:
 *
 *     hashsetPosition position;
 *     void *existing;
 *     if (hashsetFindPositionForInsert(s, key, &position, &existing)) {
 *         // Position found where we can insert an element with this key.
 *         void *element = createNewElementWithKeyAndValue(key, some_value);
 *         hashsetInsertAtPosition(s, element, &position);
 *     } else {
 *         // Existing element found with the matching key.
 *         doSomethingWithExistingElement(existing);
 *     }
 */
int hashsetFindPositionForInsert(hashset *s, void *key, hashsetPosition *position, void **existing) {
    uint64_t hash = hashKey(s, key);
    int pos_in_bucket, table_index;
    bucket *b = findBucket(s, hash, key, &pos_in_bucket, NULL);
    if (b != NULL) {
        if (existing) *existing = b->elements[pos_in_bucket];
        return 0;
    } else {
        hashsetExpandIfNeeded(s);
        rehashStepOnWriteIfNeeded(s);
        b = findBucketForInsert(s, hash, &pos_in_bucket, &table_index);
        assert(!isPositionFilled(b, pos_in_bucket));

        /* Store the hash bits now, so we don't need to compute the hash again
         * when hashsetInsertAtPosition() is called. */
        b->hashes[pos_in_bucket] = highBits(hash);

        /* Populate position struct. */
        assert(position != NULL);
        position->bucket = b;
        position->pos_in_bucket = pos_in_bucket;
        position->table_index = table_index;
        return 1;
    }
}

/* Inserts an element at the position previously acquired using
 * hashsetFindPositionForInsert(). The element must match the key provided when
 * finding the position. You must not access the hashset in any way between
 * hashsetFindPositionForInsert() and hashsetInsertAtPosition(), since even a
 * hashsetFind() may cause incremental rehashing to move elements in memory. */
void hashsetInsertAtPosition(hashset *s, void *element, hashsetPosition *position) {
    bucket *b = position->bucket;
    int pos_in_bucket = position->pos_in_bucket;
    int table_index = position->table_index;
    assert(!isPositionFilled(b, pos_in_bucket));
    b->presence |= (1 << pos_in_bucket);
    b->elements[pos_in_bucket] = element;
    s->used[table_index]++;
    /* Hash bits are already set by hashsetFindPositionForInsert. */
}

/* Add or overwrite. Returns 1 if an new element was inserted, 0 if an existing
 * element was overwritten. */
int hashsetReplace(hashset *s, void *element) {
    const void *key = elementGetKey(s, element);
    int pos_in_bucket = 0;
    uint64_t hash = hashKey(s, key);
    bucket *b = findBucket(s, hash, key, &pos_in_bucket, NULL);
    if (b != NULL) {
        freeElement(s, b->elements[pos_in_bucket]);
        b->elements[pos_in_bucket] = element;
        return 0;
    } else {
        insert(s, hash, element);
        return 1;
    }
}

/* Removes the element with the matching key and returns it. The element
 * destructor is not called. Returns 1 and points 'popped' to the element if a
 * matching element was found. Returns 0 if no matching element was found. */
int hashsetPop(hashset *s, const void *key, void **popped) {
    if (hashsetSize(s) == 0) return 0;
    uint64_t hash = hashKey(s, key);
    int pos_in_bucket = 0;
    int table_index = 0;
    bucket *b = findBucket(s, hash, key, &pos_in_bucket, &table_index);
    if (b) {
        if (popped) *popped = b->elements[pos_in_bucket];
        b->presence &= ~(1 << pos_in_bucket);
        s->used[table_index]--;
        if (b->chained && !hashsetIsRehashingPaused(s)) {
            /* Rehashing is paused while iterating and when a scan callback is
             * running. In those cases, we do the compaction in the scan and
             * interator code instead. */
            fillBucketHole(s, b, pos_in_bucket, table_index);
        }
        hashsetShrinkIfNeeded(s);
        return 1;
    } else {
        return 0;
    }
}

/* Deletes the element with the matching key. Returns 1 if an element was
 * deleted, 0 if no matching element was found. */
int hashsetDelete(hashset *s, const void *key) {
    void *element;
    if (hashsetPop(s, key, &element)) {
        freeElement(s, element);
        return 1;
    } else {
        return 0;
    }
}

/* Two-phase pop: Look up an element, do something with it, then delete it
 * without searching the hash table again.
 *
 * hashsetTwoPhasePopFindRef finds an element in the table and also the position
 * of the element within the table, so that it can be deleted without looking it
 * up in the table again. The function returns a pointer to the element the
 * element pointer within the hash table, if an element with a matching key is
 * found, and NULL otherwise.
 *
 * If non-NULL is returned, call 'hashsetTwoPhasePopDelete' with the returned
 * 'position' afterwards to actually delete the element from the table. These
 * two functions are designed be used in pair. `hashsetTwoPhasePopFindRef`
 * pauses rehashing and `hashsetTwoPhasePopDelete` resumes rehashing.
 *
 * While hashsetPop finds and returns an element, the purpose of two-phase pop
 * is to provide an optimized equivalent of hashsetFindRef followed by
 * hashsetDelete, where the first call finds the element but doesn't delete it
 * from the hash table and the latter doesn't need to look up the element in the
 * hash table again.
 *
 * Example:
 *
 *     hashsetPosition position;
 *     void **ref = hashsetTwoPhasePopFindRef(s, key, &position)
 *     if (ref != NULL) {
 *         void *element = *ref;
 *         // do something with the element, then...
 *         hashsetTwoPhasePopDelete(s, &position);
 *     }
 */

/* Like hashsetTwoPhasePopFind, but returns a pointer to where the element is
 * stored in the table, or NULL if no matching element is found. The 'position'
 * argument is populated with a representation of where the element is stored.
 * This must be provided to hashsetTwoPhasePopDelete to complete the
 * operation. */
void **hashsetTwoPhasePopFindRef(hashset *s, const void *key, hashsetPosition *position) {
    if (hashsetSize(s) == 0) return NULL;
    uint64_t hash = hashKey(s, key);
    int pos_in_bucket = 0;
    int table_index = 0;
    bucket *b = findBucket(s, hash, key, &pos_in_bucket, &table_index);
    if (b) {
        hashsetPauseRehashing(s);

        /* Store position. */
        assert(position != NULL);
        position->bucket = b;
        position->pos_in_bucket = pos_in_bucket;
        position->table_index = table_index;
        return &b->elements[pos_in_bucket];
    } else {
        return NULL;
    }
}

/* Clears the position of the element in the hashset and resumes rehashing. The
 * element destructor is NOT called. The position is acquired using a preceding
 * call to hashsetTwoPhasePopFindRef(). */
void hashsetTwoPhasePopDelete(hashset *s, hashsetPosition *position) {
    /* Read position. */
    bucket *b = position->bucket;
    int pos_in_bucket = position->pos_in_bucket;
    int table_index = position->table_index;

    /* Delete the element and resume rehashing. */
    assert(isPositionFilled(b, pos_in_bucket));
    b->presence &= ~(1 << pos_in_bucket);
    s->used[table_index]--;
    hashsetShrinkIfNeeded(s);
    hashsetResumeRehashing(s);
    if (b->chained && !hashsetIsRehashingPaused(s)) {
        /* Rehashing paused also means bucket chain compaction paused. It is
         * paused while iterating and when a scan callback is running, to be
         * able to live up to the scan and iterator guarantes. In those cases,
         * we do the compaction in the scan and interator code instead. */
        fillBucketHole(s, b, pos_in_bucket, table_index);
    }
}

/* --- Scan --- */

/* Scan is a stateless iterator. It works with a cursor that is returned to the
 * caller and which should be provided to the next call to continue scanning.
 * The hash table can be modified in any way between two scan calls. The scan
 * still continues iterating where it was.
 *
 * A full scan is performed like this: Start with a cursor of 0. The scan
 * callback is invoked for each element scanned and a new cursor is returned.
 * Next time, call this function with the new cursor. Continue until the
 * function returns 0.
 *
 * We say that an element is *emitted* when it's passed to the scan callback.
 *
 * Scan guarantees:
 *
 * - An element that is present in the hash table during an entire full scan
 *   will be returned (emitted) at least once. (Most of the time exactly once,
 *   but sometimes twice.)
 *
 * - An element that is inserted or deleted during a full scan may or may not be
 *   returned during the scan.
 *
 * Scan callback rules:
 *
 * - The scan callback may delete the element that was passed to it.
 *
 * - It may not delete other elements, because that may lead to internal
 *   fragmentation in the form of "holes" in the bucket chains.
 *
 * - The scan callback may insert or replace any element.
 */
size_t hashsetScan(hashset *s, size_t cursor, hashsetScanFunction fn, void *privdata) {
    return hashsetScanDefrag(s, cursor, fn, privdata, NULL, 0);
}

/* Like hashsetScan, but additionally reallocates the memory used by the dict
 * entries using the provided allocation function. This feature was added for
 * the active defrag feature.
 *
 * The 'defragfn' callback is called with a pointer to memory that callback can
 * reallocate. The callbacks should return a new memory address or NULL, where
 * NULL means that no reallocation happened and the old memory is still
 * valid. The 'defragfn' can be NULL if you don't need defrag reallocation.
 *
 * The 'flags' argument can be used to tweak the behaviour. It's a bitwise-or
 * (zero means no flags) of the following:
 *
 * - HASHSET_SCAN_EMIT_REF: Emit a pointer to the element's location in the
 *   table to the scan function instead of the actual element. This can be used
 *   for advanced things like reallocating the memory of an element (for the
 *   purpose of defragmentation) and updating the pointer to the element inside
 *   the hash table.
 */
size_t hashsetScanDefrag(hashset *s, size_t cursor, hashsetScanFunction fn, void *privdata,
                         void *(*defragfn)(void *), int flags) {
    if (hashsetSize(s) == 0) return 0;

    /* Prevent elements from being moved around during the scan call, as a
     * side-effect of the scan callback. */
    hashsetPauseRehashing(s);

    /* Flags. */
    int emit_ref = (flags & HASHSET_SCAN_EMIT_REF);

    if (!hashsetIsRehashing(s)) {
        /* Emit elements at the cursor index. */
        size_t mask = expToMask(s->bucket_exp[0]);
        bucket *b = &s->tables[0][cursor & mask];
        do {
            if (b->presence != 0) {
                int pos;
                for (pos = 0; pos < ELEMENTS_PER_BUCKET; pos++) {
                    if (isPositionFilled(b, pos)) {
                        void *emit = emit_ref ? &b->elements[pos] : b->elements[pos];
                        fn(privdata, emit);
                    }
                }
            }
            bucket *next = bucketNext(b);
            if (next != NULL && defragfn != NULL) {
                next = bucketDefrag(b, next, defragfn);
            }
            b = next;
        } while (b != NULL);

        /* Advance cursor. */
        cursor = nextCursor(cursor, mask);
    } else {
        int table_small, table_large;
        if (s->bucket_exp[0] <= s->bucket_exp[1]) {
            table_small = 0;
            table_large = 1;
        } else {
            table_small = 1;
            table_large = 0;
        }

        size_t mask_small = expToMask(s->bucket_exp[table_small]);
        size_t mask_large = expToMask(s->bucket_exp[table_large]);

        /* Emit elements in the smaller table, if this index hasn't already been
         * rehashed. */
        size_t idx = cursor & mask_small;
        if (table_small == 1 || s->rehash_idx == -1 || idx >= (size_t)s->rehash_idx) {
            size_t used_before = s->used[table_small];
            bucket *b = &s->tables[table_small][idx];
            do {
                if (b->presence) {
                    for (int pos = 0; pos < ELEMENTS_PER_BUCKET; pos++) {
                        if (isPositionFilled(b, pos)) {
                            void *emit = emit_ref ? &b->elements[pos] : b->elements[pos];
                            fn(privdata, emit);
                        }
                    }
                }
                bucket *next = bucketNext(b);
                if (next != NULL && defragfn != NULL) {
                    next = bucketDefrag(b, next, defragfn);
                }
                b = next;
            } while (b != NULL);
            /* If any elements were deleted, fill the holes. */
            if (s->used[table_small] < used_before) {
                compactBucketChain(s, idx, table_small);
            }
        }

        /* Iterate over indices in larger table that are the expansion of the
         * index pointed to by the cursor in the smaller table. */
        do {
            /* Emit elements in the larger table at this cursor, if this index
             * hash't already been rehashed. */
            idx = cursor & mask_large;
            if (table_large == 1 || s->rehash_idx == -1 || idx >= (size_t)s->rehash_idx) {
                size_t used_before = s->used[table_large];
                bucket *b = &s->tables[table_large][idx];
                do {
                    if (b->presence) {
                        for (int pos = 0; pos < ELEMENTS_PER_BUCKET; pos++) {
                            if (isPositionFilled(b, pos)) {
                                void *emit = emit_ref ? &b->elements[pos] : b->elements[pos];
                                fn(privdata, emit);
                            }
                        }
                    }
                    bucket *next = bucketNext(b);
                    if (next != NULL && defragfn != NULL) {
                        next = bucketDefrag(b, next, defragfn);
                    }
                    b = next;
                } while (b != NULL);
                /* If any elements were deleted, fill the holes. */
                if (s->used[table_large] < used_before) {
                    compactBucketChain(s, idx, table_large);
                }
            }

            /* Increment the reverse cursor not covered by the smaller mask. */
            cursor = nextCursor(cursor, mask_large);

            /* Continue while bits covered by mask difference is non-zero. */
        } while (cursor & (mask_small ^ mask_large));
    }
    hashsetResumeRehashing(s);
    return cursor;
}

/* --- Iterator --- */

/* Initiaize a iterator, that is not allowed to insert, delete or even lookup
 * elements in the hashset, because such operations can trigger incremental
 * rehashing which moves elements around and confuses the iterator. Only
 * hashsetNext is allowed. Each element is returned exactly once. Call
 * hashsetResetIterator when you are done. See also hashsetInitSafeIterator. */
void hashsetInitIterator(hashsetIterator *iter, hashset *s) {
    iter->hashset = s;
    iter->table = 0;
    iter->index = -1;
    iter->safe = 0;
}

/* Initialize a safe iterator, which is allowed to modify the hash table while
 * iterating. It pauses incremental rehashing to prevent elements from moving
 * around. Call hashsetNext to fetch each element. You must call
 * hashsetResetIterator when you are done with a safe iterator.
 *
 * It's allowed to insert and replace elements. Deleting elements is only
 * allowed for the element that was just returned by hashsetNext. Deleting other
 * elements are not supported. (It can cause internal fragmentation.)
 *
 * Guarantees:
 *
 * - Elements that are in the hash table for the entire iteration are returned
 *   exactly once.
 *
 * - Elements that are deleted or replaced using hashsetReplace after they
 *   have been returned are not returned again.
 *
 * - Elements that are replaced using hashsetReplace before they've been
 *   returned by the iterator will be returned.
 *
 * - Elements that are inserted during the iteration may or may not be returned
 *   by the iterator.
 */
void hashsetInitSafeIterator(hashsetIterator *iter, hashset *s) {
    hashsetInitIterator(iter, s);
    iter->safe = 1;
}

/* Resets a stack-allocated iterator. */
void hashsetResetIterator(hashsetIterator *iter) {
    if (!(iter->index == -1 && iter->table == 0)) {
        if (iter->safe) {
            hashsetResumeRehashing(iter->hashset);
            assert(iter->hashset->pause_rehash >= 0);
        } else {
            assert(iter->fingerprint == hashsetFingerprint(iter->hashset));
        }
    }
}

/* Allocates and initializes an iterator. */
hashsetIterator *hashsetCreateIterator(hashset *s) {
    hashsetIterator *iter = zmalloc(sizeof(*iter));
    hashsetInitIterator(iter, s);
    return iter;
}

/* Allocates and initializes a safe iterator. */
hashsetIterator *hashsetCreateSafeIterator(hashset *s) {
    hashsetIterator *iter = hashsetCreateIterator(s);
    iter->safe = 1;
    return iter;
}

/* Resets and frees the memory of an allocated iterator, i.e. one created using
 * hashsetCreate(Safe)Iterator. */
void hashsetReleaseIterator(hashsetIterator *iter) {
    hashsetResetIterator(iter);
    zfree(iter);
}

/* Points elemptr to the next element and returns 1 if there is a next element.
 * Returns 0 if there are no more elements. */
int hashsetNext(hashsetIterator *iter, void **elemptr) {
    while (1) {
        if (iter->index == -1 && iter->table == 0) {
            /* It's the first call to next. */
            if (iter->safe) {
                hashsetPauseRehashing(iter->hashset);
                iter->last_seen_size = iter->hashset->used[iter->table];
            } else {
                iter->fingerprint = hashsetFingerprint(iter->hashset);
            }
            if (iter->hashset->tables[0] == NULL) {
                /* Empty hashset. We're done. */
                break;
            }
            iter->index = 0;
            /* Skip already rehashed buckets. */
            if (hashsetIsRehashing(iter->hashset)) {
                iter->index = iter->hashset->rehash_idx;
            }
            iter->bucket = &iter->hashset->tables[iter->table][iter->index];
            iter->pos_in_bucket = 0;
        } else {
            /* Advance to the next position within the bucket, or to the next
             * child bucket in a chain, or to the next bucket index, or to the
             * next table. */
            iter->pos_in_bucket++;
            if (iter->bucket->chained && iter->pos_in_bucket >= ELEMENTS_PER_BUCKET - 1) {
                iter->pos_in_bucket = 0;
                iter->bucket = bucketNext(iter->bucket);
            } else if (iter->pos_in_bucket >= ELEMENTS_PER_BUCKET) {
                /* Bucket index done. */
                if (iter->safe) {
                    /* If elements in this bucket chain have been deleted,
                     * they've left empty spaces in the buckets. The chain is
                     * not automatically compacted when rehashing is paused. If
                     * this iterator is the only reason for pausing rehashing,
                     * we can do the compaction now when we're done with a
                     * bucket chain, before we move on to the next index. */
                    if (iter->hashset->pause_rehash == 1 &&
                        iter->hashset->used[iter->table] < iter->last_seen_size) {
                        compactBucketChain(iter->hashset, iter->index, iter->table);
                    }
                    iter->last_seen_size = iter->hashset->used[iter->table];
                }
                iter->pos_in_bucket = 0;
                iter->index++;
                if ((size_t)iter->index >= numBuckets(iter->hashset->bucket_exp[iter->table])) {
                    if (hashsetIsRehashing(iter->hashset) && iter->table == 0) {
                        iter->index = 0;
                        iter->table++;
                    } else {
                        /* Done. */
                        break;
                    }
                }
                iter->bucket = &iter->hashset->tables[iter->table][iter->index];
            }
        }
        bucket *b = iter->bucket;
        if (!isPositionFilled(b, iter->pos_in_bucket)) {
            /* No element here. */
            continue;
        }
        /* Return the element at this position. */
        if (elemptr) {
            *elemptr = b->elements[iter->pos_in_bucket];
        }
        return 1;
    }
    return 0;
}

/* --- Random elements --- */

/* Points 'found' to a random element in the hash table and returns 1. Returns 0
 * if the table is empty. */
int hashsetRandomElement(hashset *s, void **found) {
    void *samples[WEAK_RANDOM_SAMPLE_SIZE];
    unsigned count = hashsetSampleElements(s, (void **)&samples, WEAK_RANDOM_SAMPLE_SIZE);
    if (count == 0) return 0;
    unsigned idx = random() % count;
    *found = samples[idx];
    return 1;
}

/* Points 'found' to a random element in the hash table and returns 1. Returns 0
 * if the table is empty. This one is more fair than hashsetRandomElement(). */
int hashsetFairRandomElement(hashset *s, void **found) {
    void *samples[FAIR_RANDOM_SAMPLE_SIZE];
    unsigned count = hashsetSampleElements(s, (void **)&samples, FAIR_RANDOM_SAMPLE_SIZE);
    if (count == 0) return 0;
    unsigned idx = random() % count;
    *found = samples[idx];
    return 1;
}

/* This function samples a sequence of elements starting at a random location in
 * the hash table.
 *
 * The sampled elements are stored in the array 'dst' which must have space for
 * at least 'count' elements.te
 *
 * The function returns the number of sampled elements, which is 'count' except
 * if 'count' is greater than the total number of elements in the hash table. */
unsigned hashsetSampleElements(hashset *s, void **dst, unsigned count) {
    /* Adjust count. */
    if (count > hashsetSize(s)) count = hashsetSize(s);
    scan_samples samples;
    samples.size = count;
    samples.count = 0;
    samples.elements = dst;
    size_t cursor = randomSizeT();
    while (samples.count < count) {
        cursor = hashsetScan(s, cursor, sampleElementsScanFn, &samples);
    }
    rehashStepOnReadIfNeeded(s);
    return count;
}

/* --- Stats --- */

#define HASHSET_STATS_VECTLEN 50
void hashsetFreeStats(hashsetStats *stats) {
    zfree(stats->clvector);
    zfree(stats);
}

void hashsetCombineStats(hashsetStats *from, hashsetStats *into) {
    into->toplevel_buckets += from->toplevel_buckets;
    into->child_buckets += from->child_buckets;
    into->max_chain_len = (from->max_chain_len > into->max_chain_len) ? from->max_chain_len : into->max_chain_len;
    into->size += from->size;
    into->used += from->used;
    for (int i = 0; i < HASHSET_STATS_VECTLEN; i++) {
        into->clvector[i] += from->clvector[i];
    }
}

hashsetStats *hashsetGetStatsHt(hashset *s, int table_index, int full) {
    unsigned long *clvector = zcalloc(sizeof(unsigned long) * HASHSET_STATS_VECTLEN);
    hashsetStats *stats = zcalloc(sizeof(hashsetStats));
    stats->table_index = table_index;
    stats->clvector = clvector;
    stats->toplevel_buckets = numBuckets(s->bucket_exp[table_index]);
    stats->child_buckets = s->child_buckets[table_index];
    stats->size = numBuckets(s->bucket_exp[table_index]) * ELEMENTS_PER_BUCKET;
    stats->used = s->used[table_index];
    if (!full) return stats;
    /* Compute stats about bucket chain lengths. */
    stats->max_chain_len = 0;
    for (size_t idx = 0; idx < numBuckets(s->bucket_exp[table_index]); idx++) {
        bucket *b = &s->tables[table_index][idx];
        unsigned long chainlen = 0;
        while (b->chained) {
            chainlen++;
            b = bucketNext(b);
        }
        if (chainlen > stats->max_chain_len) {
            stats->max_chain_len = chainlen;
        }
        if (chainlen >= HASHSET_STATS_VECTLEN) {
            chainlen = HASHSET_STATS_VECTLEN - 1;
        }
        clvector[chainlen]++;
    }
    return stats;
}

/* Generates human readable stats. */
size_t hashsetGetStatsMsg(char *buf, size_t bufsize, hashsetStats *stats, int full) {
    if (stats->used == 0) {
        return snprintf(buf, bufsize,
                        "Hash table %d stats (%s):\n"
                        "No stats available for empty hash tables\n",
                        stats->table_index, (stats->table_index == 0) ? "main hash table" : "rehashing target");
    }
    size_t l = 0;
    l += snprintf(buf + l, bufsize - l,
                  "Hash table %d stats (%s):\n"
                  " table size: %lu\n"
                  " number of elements: %lu\n",
                  stats->table_index,
                  (stats->table_index == 0) ? "main hash table" : "rehashing target", stats->size,
                  stats->used);
    if (full) {
        l += snprintf(buf + l, bufsize - l,
                      " top-level buckets: %lu\n"
                      " child buckets: %lu\n"
                      " max chain length: %lu\n"
                      " avg chain length: %.02f\n"
                      " chain length distribution:\n",
                      stats->toplevel_buckets,
                      stats->child_buckets,
                      stats->max_chain_len,
                      (float)stats->child_buckets / stats->toplevel_buckets);
        for (unsigned long i = 0; i < HASHSET_STATS_VECTLEN - 1; i++) {
            if (stats->clvector[i] == 0) continue;
            if (l >= bufsize) break;
            l += snprintf(buf + l, bufsize - l, "   %ld: %ld (%.02f%%)\n", i, stats->clvector[i],
                          ((float)stats->clvector[i] / stats->toplevel_buckets) * 100);
        }
    }

    /* Make sure there is a NULL term at the end. */
    buf[bufsize - 1] = '\0';
    /* Unlike snprintf(), return the number of characters actually written. */
    return strlen(buf);
}

void hashsetGetStats(char *buf, size_t bufsize, hashset *s, int full) {
    size_t l;
    char *orig_buf = buf;
    size_t orig_bufsize = bufsize;

    hashsetStats *mainHtStats = hashsetGetStatsHt(s, 0, full);
    l = hashsetGetStatsMsg(buf, bufsize, mainHtStats, full);
    hashsetFreeStats(mainHtStats);
    buf += l;
    bufsize -= l;
    if (hashsetIsRehashing(s) && bufsize > 0) {
        hashsetStats *rehashHtStats = hashsetGetStatsHt(s, 1, full);
        hashsetGetStatsMsg(buf, bufsize, rehashHtStats, full);
        hashsetFreeStats(rehashHtStats);
    }
    /* Make sure there is a NULL term at the end. */
    orig_buf[orig_bufsize - 1] = '\0';
}

/* --- DEBUG --- */

void hashsetDump(hashset *s) {
    for (int table = 0; table <= 1; table++) {
        printf("Table %d, used %zu, exp %d, top-level buckets %zu, child buckets %zu\n",
               table, s->used[table], s->bucket_exp[table],
               numBuckets(s->bucket_exp[table]), s->child_buckets[table]);
        for (size_t idx = 0; idx < numBuckets(s->bucket_exp[table]); idx++) {
            bucket *b = &s->tables[table][idx];
            int level = 0;
            do {
                printf("Bucket %d:%zu level:%d\n", table, idx, level);
                for (int pos = 0; pos < ELEMENTS_PER_BUCKET; pos++) {
                    printf("  %d ", pos);
                    if (isPositionFilled(b, pos)) {
                        printf("h2 %02x, key \"%s\"\n", b->hashes[pos],
                               (const char *)elementGetKey(s, b->elements[pos]));
                    } else {
                        printf("(empty)\n");
                    }
                }
                b = bucketNext(b);
                level++;
            } while (b != NULL);
        }
    }
}

/* Prints a histogram-like view of the number of elements in each bucket and
 * sub-bucket. Example:
 *
 *     Bucket fill table=0 size=32 used=200:
 *         67453462673764475436556656776756
 *         2     3 2   3      3  45 5     3
 */
void hashsetHistogram(hashset *s) {
    for (int table = 0; table <= 1; table++) {
        if (s->bucket_exp[table] < 0) continue;
        size_t size = numBuckets(s->bucket_exp[table]);
        bucket *buckets[size];
        for (size_t idx = 0; idx < size; idx++) {
            buckets[idx] = &s->tables[table][idx];
        }
        size_t chains_left = size;
        printf("Bucket fill table=%d size=%zu used=%zu:\n",
               table, size, s->used[table]);
        do {
            printf("    ");
            for (size_t idx = 0; idx < size; idx++) {
                bucket *b = buckets[idx];
                if (b == NULL) {
                    printf(" ");
                    continue;
                }
                printf("%X", __builtin_popcount(b->presence));
                buckets[idx] = bucketNext(b);
                if (buckets[idx] == NULL) chains_left--;
            }
            printf("\n");
        } while (chains_left > 0);
    }
}

int hashsetLongestBucketChain(hashset *s) {
    int maxlen = 0;
    for (int table = 0; table <= 1; table++) {
        if (s->bucket_exp[table] < 0) {
            continue; /* table not used */
        }
        for (size_t i = 0; i < numBuckets(s->bucket_exp[table]); i++) {
            int chainlen = 0;
            bucket *b = &s->tables[table][i];
            while (b->chained) {
                if (++chainlen > maxlen) {
                    maxlen = chainlen;
                }
                b = bucketNext(b);
            }
        }
    }
    return maxlen;
}
