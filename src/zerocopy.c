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

void handleZeroCopyMessage(connection *conn) {
    struct msghdr msg;
    struct iovec iov;
    char control[1024*10];
    struct cmsghdr *cmsg;
    struct sock_extended_err *serr;

    iov.iov_base = NULL;
    iov.iov_len = 0;
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control;
    msg.msg_controllen = sizeof(control);

    if (connRecvMsg(conn, &msg, MSG_ERRQUEUE) == -1) {
        if (errno == EAGAIN) {
            /* This callback fires for all readable events, so sometimes it is a no-op. */
            return;
        }
        serverLog(LL_WARNING, "Got callback for error message but got recvmsg error: %s", strerror(errno));
        return;
    }
    for (cmsg = CMSG_FIRSTHDR(&msg); cmsg != NULL; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
        if (cmsg->cmsg_level != SOL_IP || cmsg->cmsg_type != IP_RECVERR) {
            continue;
        }
        serr = (struct sock_extended_err *) CMSG_DATA(cmsg);
        if (serr->ee_origin != SO_EE_ORIGIN_ZEROCOPY) {
            continue;
        }
        client * c = (client *) connGetPrivateData(conn);

        /* Mark the received messages as finished. */
        const uint32_t begin = serr->ee_info;
        const uint32_t end = serr->ee_data;
        for (size_t i = begin; i <= end; i++) {
            zeroCopyRecord *zcp = zeroCopyRecordBufferGet(c->zero_copy_buffer, i);
            serverAssert(zcp != NULL);
            zcp->active = 0;
        }

        /* Trim the front of the buffer up until the next outstanding write. */
        zeroCopyRecord *head = zeroCopyRecordBufferFront(c->zero_copy_buffer);
        while (head != NULL && head->active == 0) {
            if (head->last_write_for_block) {
                head->block->refcount--;
                incrementalTrimReplicationBacklog(REPL_BACKLOG_TRIM_BLOCKS_PER_CALL);
            }
            zeroCopyRecordBufferPop(c->zero_copy_buffer);
            head = zeroCopyRecordBufferFront(c->zero_copy_buffer);
        }
        if (c->zero_copy_buffer->len == 0)
            connSetErrorQueueHandler(c->conn, NULL);
    }
}
