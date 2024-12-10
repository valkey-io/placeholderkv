#include "server.h"
#include "zerocopy.h"

#ifndef HAVE_MSG_ZEROCOPY

int shouldUseZeroCopy(size_t len) {
    UNUSED(len);
    return 0;
}
ssize_t zeroCopyWriteToConn(connection *conn, char* data, size_t len) {
    UNUSED(conn);
    UNUSED(data);
    UNUSED(len);
    return -1;
}
zeroCopyTracker *createZeroCopyTracker() { return NULL; }
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
void processZeroCopyMessages(connection *conn) {
    UNUSED(conn);
}

#else

#include <linux/errqueue.h>

int shouldUseZeroCopy(size_t len) {
    return server.tcp_tx_zerocopy && len >= ZERO_COPY_MIN_WRITE_SIZE;
}

ssize_t zeroCopyWriteToConn(connection *conn, char* data, size_t len) {
    return connSend(conn, data, len, MSG_ZEROCOPY);
}

zeroCopyTracker *createZeroCopyTracker() {
    zeroCopyTracker *result = (zeroCopyTracker *) zmalloc(sizeof(zeroCopyTracker));
    result->start = 0;
    result->len = 0;
    result->capacity = ZERO_COPY_RECORD_TRACKER_INIT_SIZE;
    result->records = (zeroCopyRecord *) zmalloc(sizeof(zeroCopyRecord) * ZERO_COPY_RECORD_TRACKER_INIT_SIZE);
    return result;
}

zeroCopyRecord *zeroCopyTrackerGet(zeroCopyTracker *tracker, uint32_t index) {
    /* These checks need to be resilient to wraparound, which will happen after ~4B writes. */
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
    tracker->records = zmalloc(target_capacity * sizeof(zeroCopyRecord));
    /* Note this loop needs to be resilient to wraparound. */
    for (uint32_t i = tracker->start; i != tracker->start + tracker->len; i++) {
        tracker->records[i % target_capacity] = old[i % old_capacity];
    }
    tracker->capacity = target_capacity;
    zfree(old);
}

void zeroCopyTrackerPop(zeroCopyTracker *tracker) {
    tracker->start++;
    tracker->len--;
    if (tracker->capacity > ZERO_COPY_RECORD_TRACKER_INIT_SIZE && tracker->len <= tracker->capacity * ZERO_COPY_DOWNSIZE_UTILIZATION_WATERMARK) {
        zeroCopyTrackerResize(tracker, tracker->capacity / 2);
    }
}

zeroCopyRecord *zeroCopyTrackerExtend(zeroCopyTracker *tracker) {
    if (tracker->len == tracker->capacity) {
        zeroCopyTrackerResize(tracker, tracker->capacity * 2);
    }
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
    zfree(tracker);
}

void zeroCopyTrackerProcessNotifications(zeroCopyTracker *tracker, connection *conn) {
    struct msghdr msg;
    memset(&msg, 0, sizeof(msg));
    struct iovec iov;
    char control[48 * 10];
    struct cmsghdr *cmsg;
    struct sock_extended_err *serr;

    iov.iov_base = NULL;
    iov.iov_len = 0;
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control;
    msg.msg_controllen = sizeof(control);

    serverLog(LL_WARNING, "Handling zero copy messages");

    if (connRecvMsg(conn, &msg, MSG_ERRQUEUE) == -1) {
        if (errno == EAGAIN) {
            serverLog(LL_WARNING, "No zero copy messages");
            return;
        }
        serverLog(LL_WARNING, "Got callback for error message but got recvmsg error: %s", strerror(errno));
        return;
    }
    for (cmsg = CMSG_FIRSTHDR(&msg); cmsg != NULL; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
        serverLog(LL_WARNING, "Handling CMSG: len: %lu", cmsg->cmsg_len);
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
        serverLog(LL_WARNING, "CMSG is zero copy for %u to %u", begin, end);

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
}

void zeroCopyDrainConnection(connection *conn) {
    serverLog(LL_WARNING, "Handling drain callback on fd %d", conn->fd);
    zeroCopyTracker *tracker = connGetPrivateData(conn);
    zeroCopyTrackerProcessNotifications(tracker, conn);
    if (tracker->len == 0) {
        serverLog(LL_WARNING, "Done zcp draining on fd %d", conn->fd);
        connClose(conn);
        freeZeroCopyTracker(tracker);
        server.draining_zero_copy_connections--;
    }
}

/* With zero copy enabled, connection teardown will attempt to cancel any unsent packets and
 * trigger immediate notifications where possible. However, it is also possible that some packets
 * are already sent and waiting ACK and won't be freed in case of retransmission. This means we
 * cannot immediately drop all references on teardown, and instead have to wait for the tracker's
 * length to hit zero. */
void zeroCopyStartDraining(zeroCopyTracker *tracker, connection *conn) {
    // zeroCopyDrainingConnection *to_drain = (zeroCopyDrainingConnection*) zmalloc(sizeof(zeroCopyDrainingConnection));
    // to_drain->conn = conn;
    // to_drain->tracker = tracker;
    // listAddNodeHead(server.draining_zero_copy_connections, to_drain);

    /* From this point on, the connection only handles the outgoing zero copy notifications. */
    connSetWriteHandler(conn, NULL);
    connSetReadHandler(conn, NULL);
    connSetErrorQueueHandler(conn, zeroCopyDrainConnection);
    connSetPrivateData(conn, (void *) tracker);
    server.draining_zero_copy_connections++;
    zeroCopyDrainConnection(conn);
}

void processZeroCopyMessages(connection *conn) {
    client * c = (client *) connGetPrivateData(conn);
    zeroCopyTrackerProcessNotifications(c->zero_copy_tracker, conn);
}

#endif