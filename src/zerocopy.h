#ifndef ZEROCOPY_H
#define ZEROCOPY_H

#include "server.h"

#define ZERO_COPY_RECORD_TRACKER_INIT_SIZE 1024
#define ZERO_COPY_DOWNSIZE_UTILIZATION_WATERMARK 0.4

int shouldUseZeroCopy(connection *conn, size_t len);
ssize_t zeroCopyWriteToConn(connection *conn, char *buf, size_t len);
zeroCopyTracker *createZeroCopyTracker(void);
void freeZeroCopyTracker(zeroCopyTracker *tracker);
zeroCopyRecord *zeroCopyTrackerGet(zeroCopyTracker *tracker, uint32_t index);
zeroCopyRecord *zeroCopyTrackerFront(zeroCopyTracker *tracker);
void zeroCopyTrackerPop(zeroCopyTracker *tracker);
zeroCopyRecord *zeroCopyTrackerExtend(zeroCopyTracker *tracker);
zeroCopyRecord *zeroCopyTrackerEnd(zeroCopyTracker *tracker);
void zeroCopyStartDraining(client *c);

/* Callback for when there is a new message on the connection's message queue.
 * Assumes that the client object is stored as private data in the connection. */
void processZeroCopyMessages(connection *conn);

#endif /* ZEROCOPY_H */
