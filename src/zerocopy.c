#include "server.h"
#include "zerocopy.h"
#include <linux/errqueue.h>

zeroCopyRecordBuffer *createZeroCopyRecordBuffer() {
    zeroCopyRecordBuffer *result = (zeroCopyRecordBuffer *) zmalloc(sizeof(zeroCopyRecordBuffer));
    result->start = 0;
    result->len = 0;
    result->capacity = ZERO_COPY_RECORD_BUF_INIT_SIZE;
    result->records = (zeroCopyRecord *) zmalloc(sizeof(zeroCopyRecord) * ZERO_COPY_RECORD_BUF_INIT_SIZE);
    return result;
}

zeroCopyRecord *zeroCopyRecordBufferGet(zeroCopyRecordBuffer *buf, size_t index) {
    if (index < buf->start || index >= buf->start + buf->len) {
        return NULL;
    }
    return &(buf->records[index % buf->capacity]);
}

zeroCopyRecord *zeroCopyRecordBufferFront(zeroCopyRecordBuffer *buf) {
    if (buf->len == 0) {
        return NULL;
    }
    return zeroCopyRecordBufferGet(buf, buf->start);
}

void zeroCopyRecordBufferResize(zeroCopyRecordBuffer *buf, size_t target_capacity) {
    zeroCopyRecord *old = buf->records;
    size_t old_capacity = buf->capacity;
    buf->records = zmalloc(target_capacity * sizeof(zeroCopyRecord));
    for (size_t i = buf->start; i < buf->start + buf->len; i++) {
        buf->records[i % target_capacity] = old[i % old_capacity];
    }
    buf->capacity = target_capacity;
    zfree(old);
}

void zeroCopyRecordBufferPop(zeroCopyRecordBuffer *buf) {
    buf->start++;
    buf->len--;
    if (buf->capacity > ZERO_COPY_RECORD_BUF_INIT_SIZE && buf->len <= buf->capacity / 2) {
        zeroCopyRecordBufferResize(buf, buf->capacity / 2);
    }
}

zeroCopyRecord *zeroCopyRecordBufferExtend(zeroCopyRecordBuffer *buf) {
    if (buf->len == buf->capacity) {
        zeroCopyRecordBufferResize(buf, buf->capacity * 2);
    }
    return zeroCopyRecordBufferGet(buf, buf->start + buf->len++);
}

zeroCopyRecord *zeroCopyRecordBufferEnd(zeroCopyRecordBuffer *buf) {
    if (buf->len == 0) {
        return NULL;
    }
    return zeroCopyRecordBufferGet(buf, buf->len - 1);
}

void freeZeroCopyRecordBuffer(zeroCopyRecordBuffer *buf) {
    zeroCopyRecord *head = zeroCopyRecordBufferFront(buf);
    while (head != NULL) {
        if (head->last_write_for_block) {
            serverAssert(head->block->refcount > 0);
            head->block->refcount--;
            incrementalTrimReplicationBacklog(REPL_BACKLOG_TRIM_BLOCKS_PER_CALL);
        }
        zeroCopyRecordBufferPop(buf);
        head = zeroCopyRecordBufferFront(buf);
    }
}