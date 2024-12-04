#ifndef ZEROCOPY_H
#define ZEROCOPY_H

#include "server.h"

#define ZERO_COPY_RECORD_BUF_INIT_SIZE 1024
#define ZERO_COPY_MIN_WRITE_SIZE 10*1024 /* 10KiB is the threshold referenced by the Linux kernel
                                          * as being beneficial to enable zero copy, and shows
                                          * positive performance across all tested data sizes:
                                          * https://docs.kernel.org/networking/msg_zerocopy.html */

/* Create a new zero copy record buffer with the given capacity. */
zeroCopyRecordBuffer *createZeroCopyRecordBuffer(void);

/* Free an existing zero copy buffer and decrement reference counts as needed*/
void freeZeroCopyRecordBuffer(zeroCopyRecordBuffer *buf);

/* Get a zero copy record for a specific sequence number. */
zeroCopyRecord *zeroCopyRecordBufferGet(zeroCopyRecordBuffer *buf, size_t index);

/* Get the first zero copy record in the buffer. */
zeroCopyRecord *zeroCopyRecordBufferFront(zeroCopyRecordBuffer *buf);

/* Remove the first zero copy record in the buffer. */
void zeroCopyRecordBufferPop(zeroCopyRecordBuffer *buf);

/* Add a new zero copy record to the end of the buffer. */
zeroCopyRecord *zeroCopyRecordBufferExtend(zeroCopyRecordBuffer *buf);

/* Get the last zero copy record in the buffer. */
zeroCopyRecord *zeroCopyRecordBufferEnd(zeroCopyRecordBuffer *buf);

/* Callback for when there is a new message on the connection's error queue.
 * Assumes that the client object is stored as private data in the connection. */
void handleZeroCopyMessage(connection *conn);

#endif  /* ZEROCOPY_H */