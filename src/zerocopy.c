#include "server.h"
#include "zerocopy.h"

#ifndef HAVE_MSG_ZEROCOPY

int shouldUseZeroCopy(connection *conn, size_t len) {
    UNUSED(conn);
    UNUSED(len);
    return 0;
}
ssize_t zeroCopyWriteToConn(connection *conn, char* data, size_t len) {
    UNUSED(conn);
    UNUSED(data);
    UNUSED(len);
    return -1;
}
zeroCopyTracker *createZeroCopyTracker(void) { return NULL; }
zeroCopyRecord *zeroCopyTrackerGet(zeroCopyTracker *tracker, uint32_t index) {
    UNUSED(tracker);
    UNUSED(index);
    return NULL;
}
zeroCopyRecord *zeroCopyTrackerFront(zeroCopyTracker *tracker) {
    UNUSED(tracker);
    return NULL;
}
void zeroCopyTrackerResize(zeroCopyTracker *tracker, uint32_t target_capacity) {
    UNUSED(tracker);
    UNUSED(target_capacity);
}
void zeroCopyTrackerPop(zeroCopyTracker *tracker) {
    UNUSED(tracker);
}
zeroCopyRecord *zeroCopyTrackerExtend(zeroCopyTracker *tracker) {
    UNUSED(tracker);
    return NULL;
}
zeroCopyRecord *zeroCopyTrackerEnd(zeroCopyTracker *tracker) {
    UNUSED(tracker);
    return NULL;
}
void freeZeroCopyTracker(zeroCopyTracker *tracker) {
    UNUSED(tracker);
}
void zeroCopyStartDraining(client *c) {
    UNUSED(c);
}
void processZeroCopyMessages(connection *conn) {
    UNUSED(conn);
}

#else

#include <linux/errqueue.h>

/* Zero copy is only used for TCP connections that are not on loopback, and
 * that are over the configured minimum size. */
int shouldUseZeroCopy(connection *conn, size_t len) {
    return server.tcp_tx_zerocopy && conn->type == connectionTypeTcp() &&
           len >= (size_t) server.tcp_zerocopy_min_write_size &&
           (!connIsLocal(conn) || server.debug_zerocopy_bypass_loopback_check);
}

ssize_t zeroCopyWriteToConn(connection *conn, char* data, size_t len) {
    return connSend(conn, data, len, MSG_ZEROCOPY);
}

zeroCopyTracker *createZeroCopyTracker(void) {
    zeroCopyTracker *result = (zeroCopyTracker *) zmalloc(sizeof(zeroCopyTracker));
    result->start = 0;
    result->len = 0;
    result->capacity = ZERO_COPY_RECORD_TRACKER_INIT_SIZE;
    size_t records_size = sizeof(zeroCopyRecord) * ZERO_COPY_RECORD_TRACKER_INIT_SIZE;
    result->records = (zeroCopyRecord *) zmalloc(records_size);
    result->draining = 0;
    server.stat_zero_copy_tracking_memory += sizeof(zeroCopyTracker) + records_size;
    return result;
}

zeroCopyRecord *zeroCopyTrackerGet(zeroCopyTracker *tracker, uint32_t index) {
    /* These checks need to be resilient to wraparound, which will happen after
     * ~4 billion writes. */
    serverAssert(tracker->len != 0);
    /* No wraparound case */
    serverAssert(
        tracker->start + tracker->len < tracker->start ||
        (index >= tracker->start &&
        index < tracker->start + tracker->len));
    /* Wraparound case */
    serverAssert(
        (tracker->start + tracker->len > tracker->start ||
        index >= tracker->start ||
        index < tracker->start + tracker->len));
    return &(tracker->records[index % tracker->capacity]);
}

zeroCopyRecord *zeroCopyTrackerFront(zeroCopyTracker *tracker) {
    if (tracker->len == 0) {
        return NULL;
    }
    return zeroCopyTrackerGet(tracker, tracker->start);
}

void zeroCopyTrackerResize(zeroCopyTracker *tracker, uint32_t target_capacity) {
    zeroCopyRecord *old = tracker->records;
    uint32_t old_capacity = tracker->capacity;
    size_t new_size = target_capacity * sizeof(zeroCopyRecord);
    tracker->records = zmalloc(new_size);
    /* Note this loop needs to be resilient to wraparound. */
    for (uint32_t i = tracker->start; i != tracker->start + tracker->len; i++) {
        tracker->records[i % target_capacity] = old[i % old_capacity];
    }
    tracker->capacity = target_capacity;
    zfree(old);
    server.stat_zero_copy_tracking_memory -= old_capacity * sizeof(zeroCopyRecord);
    server.stat_zero_copy_tracking_memory += new_size;
}

void zeroCopyTrackerPop(zeroCopyTracker *tracker) {
    tracker->start++;
    tracker->len--;
    server.stat_zero_copy_writes_in_flight--;
    if (tracker->capacity > ZERO_COPY_RECORD_TRACKER_INIT_SIZE && tracker->len <= tracker->capacity * ZERO_COPY_DOWNSIZE_UTILIZATION_WATERMARK) {
        zeroCopyTrackerResize(tracker, tracker->capacity / 2);
    }
}

zeroCopyRecord *zeroCopyTrackerExtend(zeroCopyTracker *tracker) {
    if (tracker->len == tracker->capacity) {
        zeroCopyTrackerResize(tracker, tracker->capacity * 2);
    }
    server.stat_zero_copy_writes_processed++;
    server.stat_zero_copy_writes_in_flight++;
    return zeroCopyTrackerGet(tracker, tracker->start + tracker->len++);
}

zeroCopyRecord *zeroCopyTrackerEnd(zeroCopyTracker *tracker) {
    if (tracker->len == 0) {
        return NULL;
    }
    return zeroCopyTrackerGet(tracker, tracker->start + tracker->len - 1);
}

void freeZeroCopyTracker(zeroCopyTracker *tracker) {
    /* Drop all referenced blocks */
    zeroCopyRecord *record = zeroCopyTrackerFront(tracker);
    while (record != NULL) {
        if (record->last_write_for_block) {
            record->block->refcount--;
        }
        zeroCopyTrackerPop(tracker);
        record = zeroCopyTrackerFront(tracker);
    }
    incrementalTrimReplicationBacklog(REPL_BACKLOG_TRIM_BLOCKS_PER_CALL);
    zfree(tracker->records);

    server.stat_zero_copy_tracking_memory -= tracker->capacity * sizeof(zeroCopyRecord);
    server.stat_zero_copy_tracking_memory -= sizeof(zeroCopyTracker);

    zfree(tracker);
}

void zeroCopyTrackerProcessNotifications(zeroCopyTracker *tracker, connection *conn) {
    struct msghdr msg = {0};
    struct iovec iov;
    char control[CMSG_SPACE(sizeof(struct sock_extended_err))];
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
        if (serr->ee_errno != 0 || serr->ee_origin != SO_EE_ORIGIN_ZEROCOPY) {
            continue;
        }

        /* Kernel provides a range of sequence numbers that are finished */
        const uint32_t begin = serr->ee_info;
        const uint32_t end = serr->ee_data;

        /* Note that integer wraparound will happen after ~4B writes on one
         * connection. This code attempts to be resilient to that by not
         * assuming begin <= end. */
        for (uint32_t i = begin; i != end + 1; i++) {
            zeroCopyRecord *zcp = zeroCopyTrackerGet(tracker, i);
            zcp->active = 0;
        }

        /* Trim the front of the tracker up until the next outstanding write. */
        zeroCopyRecord *head = zeroCopyTrackerFront(tracker);
        while (head != NULL && head->active == 0) {
            if (head->last_write_for_block) {
                head->block->refcount--;
                incrementalTrimReplicationBacklog(REPL_BACKLOG_TRIM_BLOCKS_PER_CALL);
            }
            zeroCopyTrackerPop(tracker);
            head = zeroCopyTrackerFront(tracker);
        }

        /* Deregister event if no more writes are active. */
        if (tracker->len == 0)
            connSetErrorQueueHandler(conn, NULL);
    }
    return;
}

/* Shutdown a connection and set it up to be eventually freed once all zero
 * copy messages are done. */
void zeroCopyStartDraining(client *c) {
    serverAssert(c->zero_copy_tracker && !c->zero_copy_tracker->draining);
    connShutdown(c->conn);
    c->last_interaction = server.unixtime;

    /* From this point on, the connection only handles the outgoing zero copy
     * notifications. */
    connSetWriteHandler(c->conn, NULL);
    connSetReadHandler(c->conn, NULL);
    connSetErrorQueueHandler(c->conn, processZeroCopyMessages);
    c->zero_copy_tracker->draining = 1;
    server.draining_clients++;
}

void processZeroCopyMessages(connection *conn) {
    client * c = (client *) connGetPrivateData(conn);
    serverAssert(c->zero_copy_tracker);
    zeroCopyTrackerProcessNotifications(c->zero_copy_tracker, conn);
    if (c->zero_copy_tracker->draining && c->zero_copy_tracker->len == 0) {
        c->zero_copy_tracker->draining = 0;
        server.draining_clients--;

        /* This call should actually free the client now. */
        freeClient(c);
    }
}

#endif
