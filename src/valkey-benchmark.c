/* Server benchmark utility.
 *
 * Copyright (c) 2009-2012, Redis Ltd.
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

#include "fmacros.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>
#include <signal.h>
#include <assert.h>
#include <math.h>
#include <pthread.h>
#include <stdatomic.h>
#include <argp.h>
#include <argz.h>

#include <sdscompat.h> /* Use hiredis' sds compat header that maps sds calls to their hi_ variants */
#include <sds.h>       /* Use hiredis sds. */
#include "ae.h"
#include <hiredis.h>
#ifdef USE_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <hiredis_ssl.h>
#endif
#include "adlist.h"
#include "dict.h"
#include "zmalloc.h"
#include "crc16_slottable.h"
#include "hdr_histogram.h"
#include "cli_common.h"
#include "mt19937-64.h"

#define UNUSED(V) ((void)V)
#define RANDPTR_INITIAL_SIZE 8
#define DEFAULT_LATENCY_PRECISION 3
#define MAX_LATENCY_PRECISION 4
#define MAX_THREADS 500
#define CLUSTER_SLOTS 16384
#define CONFIG_LATENCY_HISTOGRAM_MIN_VALUE 10L              /* >= 10 usecs */
#define CONFIG_LATENCY_HISTOGRAM_MAX_VALUE 3000000L         /* <= 3 secs(us precision) */
#define CONFIG_LATENCY_HISTOGRAM_INSTANT_MAX_VALUE 3000000L /* <= 3 secs(us precision) */
#define SHOW_THROUGHPUT_INTERVAL 250                        /* 250ms */

#define CLIENT_GET_EVENTLOOP(c) (c->thread_id >= 0 ? config.threads[c->thread_id]->el : config.el)

struct benchmarkThread;
struct clusterNode;
struct serverConfig;

/* Read from replica options */
typedef enum readFromReplica {
    FROM_PRIMARY_ONLY = 0, /* default option */
    FROM_REPLICA_ONLY,
    FROM_ALL
} readFromReplica;

static struct config {
    aeEventLoop *el;
    cliConnInfo conn_info;
    const char *hostsocket;
    int tls;
    struct cliSSLconfig sslconfig;
    int numclients;
    _Atomic int liveclients;
    int requests;
    _Atomic int requests_issued;
    _Atomic int requests_finished;
    _Atomic int previous_requests_finished;
    int last_printed_bytes;
    long long previous_tick;
    int keysize;
    int datasize;
    int randomkeys;
    int randomkeys_keyspacelen;
    int keepalive;
    int pipeline;
    long long start;
    long long totlatency;
    const char *title;
    list *clients;
    int quiet;
    int csv;
    int loop;
    int idlemode;
    sds input_dbnumstr;
    char *tests;
    int stdinarg; /* get last arg from stdin. (-x option) */
    int precision;
    int num_threads;
    struct benchmarkThread **threads;
    int cluster_mode;
    readFromReplica read_from_replica;
    int cluster_node_count;
    struct clusterNode **cluster_nodes;
    struct serverConfig *redis_config;
    struct hdr_histogram *latency_histogram;
    struct hdr_histogram *current_sec_latency_histogram;
    _Atomic int is_fetching_slots;
    _Atomic int is_updating_slots;
    _Atomic int slots_last_update;
    int enable_tracking;
    pthread_mutex_t liveclients_mutex;
    pthread_mutex_t is_updating_slots_mutex;
    int resp3; /* use RESP3 */
} config;

typedef struct _client {
    redisContext *context;
    sds obuf;
    char **randptr;     /* Pointers to :rand: strings inside the command buf */
    size_t randlen;     /* Number of pointers in client->randptr */
    size_t randfree;    /* Number of unused pointers in client->randptr */
    char **stagptr;     /* Pointers to slot hashtags (cluster mode only) */
    size_t staglen;     /* Number of pointers in client->stagptr */
    size_t stagfree;    /* Number of unused pointers in client->stagptr */
    size_t written;     /* Bytes of 'obuf' already written */
    long long start;    /* Start time of a request */
    long long latency;  /* Request latency */
    int pending;        /* Number of pending requests (replies to consume) */
    int prefix_pending; /* If non-zero, number of pending prefix commands. Commands
                           such as auth and select are prefixed to the pipeline of
                           benchmark commands and discarded after the first send. */
    int prefixlen;      /* Size in bytes of the pending prefix commands */
    int thread_id;
    struct clusterNode *cluster_node;
    int slots_last_update;
} *client;

/* Threads. */

typedef struct benchmarkThread {
    int index;
    pthread_t thread;
    aeEventLoop *el;
} benchmarkThread;

/* Cluster. */
typedef struct clusterNode {
    char *ip;
    int port;
    sds name;
    int flags;
    sds replicate; /* Primary ID if node is a replica */
    int *slots;
    int slots_count;
    int *updated_slots;      /* Used by updateClusterSlotsConfiguration */
    int updated_slots_count; /* Used by updateClusterSlotsConfiguration */
    int replicas_count;
    struct serverConfig *redis_config;
} clusterNode;

typedef struct serverConfig {
    sds save;
    sds appendonly;
} serverConfig;

/* Prototypes */
static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask);
static void createMissingClients(client c);
static benchmarkThread *createBenchmarkThread(int index);
static void freeBenchmarkThread(benchmarkThread *thread);
static void freeBenchmarkThreads(void);
static void *execBenchmarkThread(void *ptr);
static clusterNode *createClusterNode(char *ip, int port);
static serverConfig *getServerConfig(const char *ip, int port, const char *hostsocket);
static redisContext *getRedisContext(const char *ip, int port, const char *hostsocket);
static void freeServerConfig(serverConfig *cfg);
static int fetchClusterSlotsConfiguration(client c);
static void updateClusterSlotsConfiguration(void);
static long long showThroughput(struct aeEventLoop *eventLoop, long long id, void *clientData);

/* Dict callbacks */
static uint64_t dictSdsHash(const void *key);
static int dictSdsKeyCompare(const void *key1, const void *key2);

/* Implementation */
static long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec) * 1000000;
    ust += tv.tv_usec;
    return ust;
}

static long long mstime(void) {
    return ustime() / 1000;
}

static uint64_t dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char *)key, sdslen((char *)key));
}

static int dictSdsKeyCompare(const void *key1, const void *key2) {
    int l1, l2;
    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

static dictType dtype = {
    dictSdsHash,       /* hash function */
    NULL,              /* key dup */
    dictSdsKeyCompare, /* key compare */
    NULL,              /* key destructor */
    NULL,              /* val destructor */
    NULL               /* allow to expand */
};

static redisContext *getRedisContext(const char *ip, int port, const char *hostsocket) {
    redisContext *ctx = NULL;
    redisReply *reply = NULL;
    if (hostsocket == NULL)
        ctx = redisConnect(ip, port);
    else
        ctx = redisConnectUnix(hostsocket);
    if (ctx == NULL || ctx->err) {
        fprintf(stderr, "Could not connect to server at ");
        char *err = (ctx != NULL ? ctx->errstr : "");
        if (hostsocket == NULL)
            fprintf(stderr, "%s:%d: %s\n", ip, port, err);
        else
            fprintf(stderr, "%s: %s\n", hostsocket, err);
        goto cleanup;
    }
    if (config.tls == 1) {
        const char *err = NULL;
        if (cliSecureConnection(ctx, config.sslconfig, &err) == REDIS_ERR && err) {
            fprintf(stderr, "Could not negotiate a TLS connection: %s\n", err);
            goto cleanup;
        }
    }
    if (config.conn_info.auth == NULL) return ctx;
    if (config.conn_info.user == NULL)
        reply = redisCommand(ctx, "AUTH %s", config.conn_info.auth);
    else
        reply = redisCommand(ctx, "AUTH %s %s", config.conn_info.user, config.conn_info.auth);
    if (reply != NULL) {
        if (reply->type == REDIS_REPLY_ERROR) {
            if (hostsocket == NULL)
                fprintf(stderr, "Node %s:%d replied with error:\n%s\n", ip, port, reply->str);
            else
                fprintf(stderr, "Node %s replied with error:\n%s\n", hostsocket, reply->str);
            freeReplyObject(reply);
            redisFree(ctx);
            exit(1);
        }
        freeReplyObject(reply);
        return ctx;
    }
    fprintf(stderr, "ERROR: failed to fetch reply from ");
    if (hostsocket == NULL)
        fprintf(stderr, "%s:%d\n", ip, port);
    else
        fprintf(stderr, "%s\n", hostsocket);
cleanup:
    freeReplyObject(reply);
    redisFree(ctx);
    return NULL;
}


static serverConfig *getServerConfig(const char *ip, int port, const char *hostsocket) {
    serverConfig *cfg = zcalloc(sizeof(*cfg));
    if (!cfg) return NULL;
    redisContext *c = NULL;
    redisReply *reply = NULL, *sub_reply = NULL;
    c = getRedisContext(ip, port, hostsocket);
    if (c == NULL) {
        freeServerConfig(cfg);
        exit(1);
    }
    redisAppendCommand(c, "CONFIG GET %s", "save");
    redisAppendCommand(c, "CONFIG GET %s", "appendonly");
    int abort_test = 0;
    int i = 0;
    void *r = NULL;
    for (; i < 2; i++) {
        int res = redisGetReply(c, &r);
        if (reply) freeReplyObject(reply);
        reply = res == REDIS_OK ? ((redisReply *)r) : NULL;
        if (res != REDIS_OK || !r) goto fail;
        if (reply->type == REDIS_REPLY_ERROR) {
            goto fail;
        }
        if (reply->type != REDIS_REPLY_ARRAY || reply->elements < 2) goto fail;
        sub_reply = reply->element[1];
        char *value = sub_reply->str;
        if (!value) value = "";
        switch (i) {
        case 0: cfg->save = sdsnew(value); break;
        case 1: cfg->appendonly = sdsnew(value); break;
        }
    }
    freeReplyObject(reply);
    redisFree(c);
    return cfg;
fail:
    if (reply && reply->type == REDIS_REPLY_ERROR && !strncmp(reply->str, "NOAUTH", 6)) {
        if (hostsocket == NULL)
            fprintf(stderr, "Node %s:%d replied with error:\n%s\n", ip, port, reply->str);
        else
            fprintf(stderr, "Node %s replied with error:\n%s\n", hostsocket, reply->str);
        abort_test = 1;
    }
    freeReplyObject(reply);
    redisFree(c);
    freeServerConfig(cfg);
    if (abort_test) exit(1);
    return NULL;
}
static void freeServerConfig(serverConfig *cfg) {
    if (cfg->save) sdsfree(cfg->save);
    if (cfg->appendonly) sdsfree(cfg->appendonly);
    zfree(cfg);
}

static void freeClient(client c) {
    aeEventLoop *el = CLIENT_GET_EVENTLOOP(c);
    listNode *ln;
    aeDeleteFileEvent(el, c->context->fd, AE_WRITABLE);
    aeDeleteFileEvent(el, c->context->fd, AE_READABLE);
    if (c->thread_id >= 0) {
        int requests_finished = atomic_load_explicit(&config.requests_finished, memory_order_relaxed);
        if (requests_finished >= config.requests) {
            aeStop(el);
        }
    }
    redisFree(c->context);
    sdsfree(c->obuf);
    zfree(c->randptr);
    zfree(c->stagptr);
    zfree(c);
    if (config.num_threads) pthread_mutex_lock(&(config.liveclients_mutex));
    config.liveclients--;
    ln = listSearchKey(config.clients, c);
    assert(ln != NULL);
    listDelNode(config.clients, ln);
    if (config.num_threads) pthread_mutex_unlock(&(config.liveclients_mutex));
}

static void freeAllClients(void) {
    listNode *ln = config.clients->head, *next;

    while (ln) {
        next = ln->next;
        freeClient(ln->value);
        ln = next;
    }
}

static void resetClient(client c) {
    aeEventLoop *el = CLIENT_GET_EVENTLOOP(c);
    aeDeleteFileEvent(el, c->context->fd, AE_WRITABLE);
    aeDeleteFileEvent(el, c->context->fd, AE_READABLE);
    aeCreateFileEvent(el, c->context->fd, AE_WRITABLE, writeHandler, c);
    c->written = 0;
    c->pending = config.pipeline;
}

static void randomizeClientKey(client c) {
    size_t i;

    for (i = 0; i < c->randlen; i++) {
        char *p = c->randptr[i] + 11;
        size_t r = 0;
        if (config.randomkeys_keyspacelen != 0) r = random() % config.randomkeys_keyspacelen;
        size_t j;

        for (j = 0; j < 12; j++) {
            *p = '0' + r % 10;
            r /= 10;
            p--;
        }
    }
}

static void setClusterKeyHashTag(client c) {
    assert(c->thread_id >= 0);
    clusterNode *node = c->cluster_node;
    assert(node);
    int is_updating_slots = atomic_load_explicit(&config.is_updating_slots, memory_order_relaxed);
    /* If updateClusterSlotsConfiguration is updating the slots array,
     * call updateClusterSlotsConfiguration is order to block the thread
     * since the mutex is locked. When the slots will be updated by the
     * thread that's actually performing the update, the execution of
     * updateClusterSlotsConfiguration won't actually do anything, since
     * the updated_slots_count array will be already NULL. */
    if (is_updating_slots) updateClusterSlotsConfiguration();
    int slot = node->slots[rand() % node->slots_count];
    const char *tag = crc16_slot_table[slot];
    int taglen = strlen(tag);
    size_t i;
    for (i = 0; i < c->staglen; i++) {
        char *p = c->stagptr[i] + 1;
        p[0] = tag[0];
        p[1] = (taglen >= 2 ? tag[1] : '}');
        p[2] = (taglen == 3 ? tag[2] : '}');
    }
}

static void clientDone(client c) {
    int requests_finished = atomic_load_explicit(&config.requests_finished, memory_order_relaxed);
    if (requests_finished >= config.requests) {
        freeClient(c);
        if (!config.num_threads && config.el) aeStop(config.el);
        return;
    }
    if (config.keepalive) {
        resetClient(c);
    } else {
        if (config.num_threads) pthread_mutex_lock(&(config.liveclients_mutex));
        config.liveclients--;
        createMissingClients(c);
        config.liveclients++;
        if (config.num_threads) pthread_mutex_unlock(&(config.liveclients_mutex));
        freeClient(c);
    }
}

static void readHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    client c = privdata;
    void *reply = NULL;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    /* Calculate latency only for the first read event. This means that the
     * server already sent the reply and we need to parse it. Parsing overhead
     * is not part of the latency, so calculate it only once, here. */
    if (c->latency < 0) c->latency = ustime() - (c->start);

    if (redisBufferRead(c->context) != REDIS_OK) {
        fprintf(stderr, "Error: %s\n", c->context->errstr);
        exit(1);
    } else {
        while (c->pending) {
            if (redisGetReply(c->context, &reply) != REDIS_OK) {
                fprintf(stderr, "Error: %s\n", c->context->errstr);
                exit(1);
            }
            if (reply != NULL) {
                if (reply == (void *)REDIS_REPLY_ERROR) {
                    fprintf(stderr, "Unexpected error reply, exiting...\n");
                    exit(1);
                }
                redisReply *r = reply;
                if (r->type == REDIS_REPLY_ERROR) {
                    /* Try to update slots configuration if reply error is
                     * MOVED/ASK/CLUSTERDOWN and the key(s) used by the command
                     * contain(s) the slot hash tag.
                     * If the error is not topology-update related then we
                     * immediately exit to avoid false results. */
                    if (c->cluster_node && c->staglen) {
                        int fetch_slots = 0, do_wait = 0;
                        if (!strncmp(r->str, "MOVED", 5) || !strncmp(r->str, "ASK", 3))
                            fetch_slots = 1;
                        else if (!strncmp(r->str, "CLUSTERDOWN", 11)) {
                            /* Usually the cluster is able to recover itself after
                             * a CLUSTERDOWN error, so try to sleep one second
                             * before requesting the new configuration. */
                            fetch_slots = 1;
                            do_wait = 1;
                            fprintf(stderr, "Error from server %s:%d: %s.\n", c->cluster_node->ip,
                                    c->cluster_node->port, r->str);
                        }
                        if (do_wait) sleep(1);
                        if (fetch_slots && !fetchClusterSlotsConfiguration(c)) exit(1);
                    } else {
                        if (c->cluster_node) {
                            fprintf(stderr, "Error from server %s:%d: %s\n", c->cluster_node->ip, c->cluster_node->port,
                                    r->str);
                        } else
                            fprintf(stderr, "Error from server: %s\n", r->str);
                        exit(1);
                    }
                }

                freeReplyObject(reply);
                /* This is an OK for prefix commands such as auth and select.*/
                if (c->prefix_pending > 0) {
                    c->prefix_pending--;
                    c->pending--;
                    /* Discard prefix commands on first response.*/
                    if (c->prefixlen > 0) {
                        size_t j;
                        sdsrange(c->obuf, c->prefixlen, -1);
                        /* We also need to fix the pointers to the strings
                         * we need to randomize. */
                        for (j = 0; j < c->randlen; j++) c->randptr[j] -= c->prefixlen;
                        /* Fix the pointers to the slot hash tags */
                        for (j = 0; j < c->staglen; j++) c->stagptr[j] -= c->prefixlen;
                        c->prefixlen = 0;
                    }
                    continue;
                }
                int requests_finished = atomic_fetch_add_explicit(&config.requests_finished, 1, memory_order_relaxed);
                if (requests_finished < config.requests) {
                    if (config.num_threads == 0) {
                        hdr_record_value(config.latency_histogram, // Histogram to record to
                                         (long)c->latency <= CONFIG_LATENCY_HISTOGRAM_MAX_VALUE
                                             ? (long)c->latency
                                             : CONFIG_LATENCY_HISTOGRAM_MAX_VALUE); // Value to record
                        hdr_record_value(config.current_sec_latency_histogram,      // Histogram to record to
                                         (long)c->latency <= CONFIG_LATENCY_HISTOGRAM_INSTANT_MAX_VALUE
                                             ? (long)c->latency
                                             : CONFIG_LATENCY_HISTOGRAM_INSTANT_MAX_VALUE); // Value to record
                    } else {
                        hdr_record_value_atomic(config.latency_histogram, // Histogram to record to
                                                (long)c->latency <= CONFIG_LATENCY_HISTOGRAM_MAX_VALUE
                                                    ? (long)c->latency
                                                    : CONFIG_LATENCY_HISTOGRAM_MAX_VALUE); // Value to record
                        hdr_record_value_atomic(config.current_sec_latency_histogram,      // Histogram to record to
                                                (long)c->latency <= CONFIG_LATENCY_HISTOGRAM_INSTANT_MAX_VALUE
                                                    ? (long)c->latency
                                                    : CONFIG_LATENCY_HISTOGRAM_INSTANT_MAX_VALUE); // Value to record
                    }
                }
                c->pending--;
                if (c->pending == 0) {
                    clientDone(c);
                    break;
                }
            } else {
                break;
            }
        }
    }
}

static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    client c = privdata;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    /* Initialize request when nothing was written. */
    if (c->written == 0) {
        /* Enforce upper bound to number of requests. */
        int requests_issued = atomic_fetch_add_explicit(&config.requests_issued, config.pipeline, memory_order_relaxed);
        if (requests_issued >= config.requests) {
            return;
        }

        /* Really initialize: randomize keys and set start time. */
        if (config.randomkeys) randomizeClientKey(c);
        if (config.cluster_mode && c->staglen > 0) setClusterKeyHashTag(c);
        c->slots_last_update = atomic_load_explicit(&config.slots_last_update, memory_order_relaxed);
        c->start = ustime();
        c->latency = -1;
    }
    const ssize_t buflen = sdslen(c->obuf);
    const ssize_t writeLen = buflen - c->written;
    if (writeLen > 0) {
        void *ptr = c->obuf + c->written;
        while (1) {
            /* Optimistically try to write before checking if the file descriptor
             * is actually writable. At worst we get EAGAIN. */
            const ssize_t nwritten = cliWriteConn(c->context, ptr, writeLen);
            if (nwritten != writeLen) {
                if (nwritten == -1 && errno != EAGAIN) {
                    if (errno != EPIPE) fprintf(stderr, "Error writing to the server: %s\n", strerror(errno));
                    freeClient(c);
                    return;
                } else if (nwritten > 0) {
                    c->written += nwritten;
                    return;
                }
            } else {
                aeDeleteFileEvent(el, c->context->fd, AE_WRITABLE);
                aeCreateFileEvent(el, c->context->fd, AE_READABLE, readHandler, c);
                return;
            }
        }
    }
}

/* Create a benchmark client, configured to send the command passed as 'cmd' of
 * 'len' bytes.
 *
 * The command is copied N times in the client output buffer (that is reused
 * again and again to send the request to the server) accordingly to the configured
 * pipeline size.
 *
 * Also an initial SELECT command is prepended in order to make sure the right
 * database is selected, if needed. The initial SELECT will be discarded as soon
 * as the first reply is received.
 *
 * To create a client from scratch, the 'from' pointer is set to NULL. If instead
 * we want to create a client using another client as reference, the 'from' pointer
 * points to the client to use as reference. In such a case the following
 * information is take from the 'from' client:
 *
 * 1) The command line to use.
 * 2) The offsets of the __rand_int__ elements inside the command line, used
 *    for arguments randomization.
 *
 * Even when cloning another client, prefix commands are applied if needed.*/
static client createClient(char *cmd, size_t len, client from, int thread_id) {
    int j;
    int is_cluster_client = (config.cluster_mode && thread_id >= 0);
    client c = zmalloc(sizeof(struct _client));

    const char *ip = NULL;
    int port = 0;
    c->cluster_node = NULL;
    if (config.hostsocket == NULL || is_cluster_client) {
        if (!is_cluster_client) {
            ip = config.conn_info.hostip;
            port = config.conn_info.hostport;
        } else {
            int node_idx = 0;
            if (config.num_threads < config.cluster_node_count)
                node_idx = config.liveclients % config.cluster_node_count;
            else
                node_idx = thread_id % config.cluster_node_count;
            clusterNode *node = config.cluster_nodes[node_idx];
            assert(node != NULL);
            ip = (const char *)node->ip;
            port = node->port;
            c->cluster_node = node;
        }
        c->context = redisConnectNonBlock(ip, port);
    } else {
        c->context = redisConnectUnixNonBlock(config.hostsocket);
    }
    if (c->context->err) {
        fprintf(stderr, "Could not connect to server at ");
        if (config.hostsocket == NULL || is_cluster_client)
            fprintf(stderr, "%s:%d: %s\n", ip, port, c->context->errstr);
        else
            fprintf(stderr, "%s: %s\n", config.hostsocket, c->context->errstr);
        exit(1);
    }
    if (config.tls == 1) {
        const char *err = NULL;
        if (cliSecureConnection(c->context, config.sslconfig, &err) == REDIS_ERR && err) {
            fprintf(stderr, "Could not negotiate a TLS connection: %s\n", err);
            exit(1);
        }
    }
    c->thread_id = thread_id;
    /* Suppress hiredis cleanup of unused buffers for max speed. */
    c->context->reader->maxbuf = 0;

    /* Build the request buffer:
     * Queue N requests accordingly to the pipeline size, or simply clone
     * the example client buffer. */
    c->obuf = sdsempty();
    /* Prefix the request buffer with AUTH and/or SELECT commands, if applicable.
     * These commands are discarded after the first response, so if the client is
     * reused the commands will not be used again. */
    c->prefix_pending = 0;
    if (config.conn_info.auth) {
        char *buf = NULL;
        int len;
        if (config.conn_info.user == NULL)
            len = redisFormatCommand(&buf, "AUTH %s", config.conn_info.auth);
        else
            len = redisFormatCommand(&buf, "AUTH %s %s", config.conn_info.user, config.conn_info.auth);
        c->obuf = sdscatlen(c->obuf, buf, len);
        free(buf);
        c->prefix_pending++;
    }

    if (config.enable_tracking) {
        char *buf = NULL;
        int len = redisFormatCommand(&buf, "CLIENT TRACKING on");
        c->obuf = sdscatlen(c->obuf, buf, len);
        free(buf);
        c->prefix_pending++;
    }

    /* If a DB number different than zero is selected, prefix our request
     * buffer with the SELECT command, that will be discarded the first
     * time the replies are received, so if the client is reused the
     * SELECT command will not be used again. */
    if (config.conn_info.input_dbnum != 0 && !is_cluster_client) {
        c->obuf = sdscatprintf(c->obuf, "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n", (int)sdslen(config.input_dbnumstr),
                               config.input_dbnumstr);
        c->prefix_pending++;
    }

    if (config.resp3) {
        char *buf = NULL;
        int len = redisFormatCommand(&buf, "HELLO 3");
        c->obuf = sdscatlen(c->obuf, buf, len);
        free(buf);
        c->prefix_pending++;
    }

    if (config.cluster_mode && (config.read_from_replica == FROM_REPLICA_ONLY || config.read_from_replica == FROM_ALL)) {
        char *buf = NULL;
        int len;
        len = redisFormatCommand(&buf, "READONLY");
        c->obuf = sdscatlen(c->obuf, buf, len);
        free(buf);
        c->prefix_pending++;
    }

    c->prefixlen = sdslen(c->obuf);
    /* Append the request itself. */
    if (from) {
        c->obuf = sdscatlen(c->obuf, from->obuf + from->prefixlen, sdslen(from->obuf) - from->prefixlen);
    } else {
        for (j = 0; j < config.pipeline; j++) c->obuf = sdscatlen(c->obuf, cmd, len);
    }

    c->written = 0;
    c->pending = config.pipeline + c->prefix_pending;
    c->randptr = NULL;
    c->randlen = 0;
    c->stagptr = NULL;
    c->staglen = 0;

    /* Find substrings in the output buffer that need to be randomized. */
    if (config.randomkeys) {
        if (from) {
            c->randlen = from->randlen;
            c->randfree = 0;
            c->randptr = zmalloc(sizeof(char *) * c->randlen);
            /* copy the offsets. */
            for (j = 0; j < (int)c->randlen; j++) {
                c->randptr[j] = c->obuf + (from->randptr[j] - from->obuf);
                /* Adjust for the different select prefix length. */
                c->randptr[j] += c->prefixlen - from->prefixlen;
            }
        } else {
            char *p = c->obuf;

            c->randlen = 0;
            c->randfree = RANDPTR_INITIAL_SIZE;
            c->randptr = zmalloc(sizeof(char *) * c->randfree);
            while ((p = strstr(p, "__rand_int__")) != NULL) {
                if (c->randfree == 0) {
                    c->randptr = zrealloc(c->randptr, sizeof(char *) * c->randlen * 2);
                    c->randfree += c->randlen;
                }
                c->randptr[c->randlen++] = p;
                c->randfree--;
                p += 12; /* 12 is strlen("__rand_int__). */
            }
        }
    }
    /* If cluster mode is enabled, set slot hashtags pointers. */
    if (config.cluster_mode) {
        if (from) {
            c->staglen = from->staglen;
            c->stagfree = 0;
            c->stagptr = zmalloc(sizeof(char *) * c->staglen);
            /* copy the offsets. */
            for (j = 0; j < (int)c->staglen; j++) {
                c->stagptr[j] = c->obuf + (from->stagptr[j] - from->obuf);
                /* Adjust for the different select prefix length. */
                c->stagptr[j] += c->prefixlen - from->prefixlen;
            }
        } else {
            char *p = c->obuf;

            c->staglen = 0;
            c->stagfree = RANDPTR_INITIAL_SIZE;
            c->stagptr = zmalloc(sizeof(char *) * c->stagfree);
            while ((p = strstr(p, "{tag}")) != NULL) {
                if (c->stagfree == 0) {
                    c->stagptr = zrealloc(c->stagptr, sizeof(char *) * c->staglen * 2);
                    c->stagfree += c->staglen;
                }
                c->stagptr[c->staglen++] = p;
                c->stagfree--;
                p += 5; /* 5 is strlen("{tag}"). */
            }
        }
    }
    aeEventLoop *el = NULL;
    if (thread_id < 0)
        el = config.el;
    else {
        benchmarkThread *thread = config.threads[thread_id];
        el = thread->el;
    }
    if (config.idlemode == 0)
        aeCreateFileEvent(el, c->context->fd, AE_WRITABLE, writeHandler, c);
    else
        /* In idle mode, clients still need to register readHandler for catching errors */
        aeCreateFileEvent(el, c->context->fd, AE_READABLE, readHandler, c);

    listAddNodeTail(config.clients, c);
    atomic_fetch_add_explicit(&config.liveclients, 1, memory_order_relaxed);

    c->slots_last_update = atomic_load_explicit(&config.slots_last_update, memory_order_relaxed);
    return c;
}

static void createMissingClients(client c) {
    int n = 0;
    while (config.liveclients < config.numclients) {
        int thread_id = -1;
        if (config.num_threads) thread_id = config.liveclients % config.num_threads;
        createClient(NULL, 0, c, thread_id);

        /* Listen backlog is quite limited on most systems */
        if (++n > 64) {
            usleep(50000);
            n = 0;
        }
    }
}

static void showLatencyReport(void) {
    const float reqpersec = (float)config.requests_finished / ((float)config.totlatency / 1000.0f);
    const float p0 = ((float)hdr_min(config.latency_histogram)) / 1000.0f;
    const float p50 = hdr_value_at_percentile(config.latency_histogram, 50.0) / 1000.0f;
    const float p95 = hdr_value_at_percentile(config.latency_histogram, 95.0) / 1000.0f;
    const float p99 = hdr_value_at_percentile(config.latency_histogram, 99.0) / 1000.0f;
    const float p100 = ((float)hdr_max(config.latency_histogram)) / 1000.0f;
    const float avg = hdr_mean(config.latency_histogram) / 1000.0f;

    if (!config.quiet && !config.csv) {
        printf("%*s\r", config.last_printed_bytes, " "); // ensure there is a clean line
        printf("====== %s ======\n", config.title);
        printf("  %d requests completed in %.2f seconds\n", config.requests_finished, (float)config.totlatency / 1000);
        printf("  %d parallel clients\n", config.numclients);
        printf("  %d bytes payload\n", config.datasize);
        printf("  keep alive: %d\n", config.keepalive);
        if (config.cluster_mode) {
            const char *node_roles = NULL;
            if (config.read_from_replica == FROM_ALL) {
                node_roles = "cluster";
            } else if (config.read_from_replica == FROM_REPLICA_ONLY) {
                node_roles = "replica";
            } else {
                node_roles = "primary";
            }
            printf("  cluster mode: yes (%d %s)\n", config.cluster_node_count, node_roles);
            int m;
            for (m = 0; m < config.cluster_node_count; m++) {
                clusterNode *node = config.cluster_nodes[m];
                serverConfig *cfg = node->redis_config;
                if (cfg == NULL) continue;
                printf("  node [%d] configuration:\n", m);
                printf("    save: %s\n", sdslen(cfg->save) ? cfg->save : "NONE");
                printf("    appendonly: %s\n", cfg->appendonly);
            }
        } else {
            if (config.redis_config) {
                printf("  host configuration \"save\": %s\n", config.redis_config->save);
                printf("  host configuration \"appendonly\": %s\n", config.redis_config->appendonly);
            }
        }
        printf("  multi-thread: %s\n", (config.num_threads ? "yes" : "no"));
        if (config.num_threads) printf("  threads: %d\n", config.num_threads);

        printf("\n");
        printf("Latency by percentile distribution:\n");
        struct hdr_iter iter;
        long long previous_cumulative_count = -1;
        const long long total_count = config.latency_histogram->total_count;
        hdr_iter_percentile_init(&iter, config.latency_histogram, 1);
        struct hdr_iter_percentiles *percentiles = &iter.specifics.percentiles;
        while (hdr_iter_next(&iter)) {
            const double value = iter.highest_equivalent_value / 1000.0f;
            const double percentile = percentiles->percentile;
            const long long cumulative_count = iter.cumulative_count;
            if (previous_cumulative_count != cumulative_count || cumulative_count == total_count) {
                printf("%3.3f%% <= %.3f milliseconds (cumulative count %lld)\n", percentile, value, cumulative_count);
            }
            previous_cumulative_count = cumulative_count;
        }
        printf("\n");
        printf("Cumulative distribution of latencies:\n");
        previous_cumulative_count = -1;
        hdr_iter_linear_init(&iter, config.latency_histogram, 100);
        while (hdr_iter_next(&iter)) {
            const double value = iter.highest_equivalent_value / 1000.0f;
            const long long cumulative_count = iter.cumulative_count;
            const double percentile = ((double)cumulative_count / (double)total_count) * 100.0;
            if (previous_cumulative_count != cumulative_count || cumulative_count == total_count) {
                printf("%3.3f%% <= %.3f milliseconds (cumulative count %lld)\n", percentile, value, cumulative_count);
            }
            /* After the 2 milliseconds latency to have percentages split
             * by decimals will just add a lot of noise to the output. */
            if (iter.highest_equivalent_value > 2000) {
                hdr_iter_linear_set_value_units_per_bucket(&iter, 1000);
            }
            previous_cumulative_count = cumulative_count;
        }
        printf("\n");
        printf("Summary:\n");
        printf("  throughput summary: %.2f requests per second\n", reqpersec);
        printf("  latency summary (msec):\n");
        printf("    %9s %9s %9s %9s %9s %9s\n", "avg", "min", "p50", "p95", "p99", "max");
        printf("    %9.3f %9.3f %9.3f %9.3f %9.3f %9.3f\n", avg, p0, p50, p95, p99, p100);
    } else if (config.csv) {
        printf("\"%s\",\"%.2f\",\"%.3f\",\"%.3f\",\"%.3f\",\"%.3f\",\"%.3f\",\"%.3f\"\n", config.title, reqpersec, avg,
               p0, p50, p95, p99, p100);
    } else {
        printf("%*s\r", config.last_printed_bytes, " "); // ensure there is a clean line
        printf("%s: %.2f requests per second, p50=%.3f msec\n", config.title, reqpersec, p50);
    }
}

static void initBenchmarkThreads(void) {
    int i;
    if (config.threads) freeBenchmarkThreads();
    config.threads = zmalloc(config.num_threads * sizeof(benchmarkThread *));
    for (i = 0; i < config.num_threads; i++) {
        benchmarkThread *thread = createBenchmarkThread(i);
        config.threads[i] = thread;
    }
}

static void startBenchmarkThreads(void) {
    int i;
    for (i = 0; i < config.num_threads; i++) {
        benchmarkThread *t = config.threads[i];
        if (pthread_create(&(t->thread), NULL, execBenchmarkThread, t)) {
            fprintf(stderr, "FATAL: Failed to start thread %d.\n", i);
            exit(1);
        }
    }
    for (i = 0; i < config.num_threads; i++) pthread_join(config.threads[i]->thread, NULL);
}

static void benchmark(const char *title, char *cmd, int len) {
    client c;

    config.title = title;
    config.requests_issued = 0;
    config.requests_finished = 0;
    config.previous_requests_finished = 0;
    config.last_printed_bytes = 0;
    hdr_init(CONFIG_LATENCY_HISTOGRAM_MIN_VALUE,         // Minimum value
             CONFIG_LATENCY_HISTOGRAM_MAX_VALUE,         // Maximum value
             config.precision,                           // Number of significant figures
             &config.latency_histogram);                 // Pointer to initialise
    hdr_init(CONFIG_LATENCY_HISTOGRAM_MIN_VALUE,         // Minimum value
             CONFIG_LATENCY_HISTOGRAM_INSTANT_MAX_VALUE, // Maximum value
             config.precision,                           // Number of significant figures
             &config.current_sec_latency_histogram);     // Pointer to initialise

    if (config.num_threads) initBenchmarkThreads();

    int thread_id = config.num_threads > 0 ? 0 : -1;
    c = createClient(cmd, len, NULL, thread_id);
    createMissingClients(c);

    config.start = mstime();
    if (!config.num_threads)
        aeMain(config.el);
    else
        startBenchmarkThreads();
    config.totlatency = mstime() - config.start;

    showLatencyReport();
    freeAllClients();
    if (config.threads) freeBenchmarkThreads();
    if (config.current_sec_latency_histogram) hdr_close(config.current_sec_latency_histogram);
    if (config.latency_histogram) hdr_close(config.latency_histogram);
}

/* Thread functions. */

static benchmarkThread *createBenchmarkThread(int index) {
    benchmarkThread *thread = zmalloc(sizeof(*thread));
    if (thread == NULL) return NULL;
    thread->index = index;
    thread->el = aeCreateEventLoop(1024 * 10);
    aeCreateTimeEvent(thread->el, 1, showThroughput, (void *)thread, NULL);
    return thread;
}

static void freeBenchmarkThread(benchmarkThread *thread) {
    if (thread->el) aeDeleteEventLoop(thread->el);
    zfree(thread);
}

static void freeBenchmarkThreads(void) {
    int i = 0;
    for (; i < config.num_threads; i++) {
        benchmarkThread *thread = config.threads[i];
        if (thread) freeBenchmarkThread(thread);
    }
    zfree(config.threads);
    config.threads = NULL;
}

static void *execBenchmarkThread(void *ptr) {
    benchmarkThread *thread = (benchmarkThread *)ptr;
    aeMain(thread->el);
    return NULL;
}

/* Cluster helper functions. */

static clusterNode *createClusterNode(char *ip, int port) {
    clusterNode *node = zmalloc(sizeof(*node));
    if (!node) return NULL;
    node->ip = ip;
    node->port = port;
    node->name = NULL;
    node->flags = 0;
    node->replicate = NULL;
    node->replicas_count = 0;
    node->slots = zmalloc(CLUSTER_SLOTS * sizeof(int));
    node->slots_count = 0;
    node->updated_slots = NULL;
    node->updated_slots_count = 0;
    node->redis_config = NULL;
    return node;
}

static void freeClusterNode(clusterNode *node) {
    if (node->name) sdsfree(node->name);
    if (node->replicate) sdsfree(node->replicate);
    /* If the node is not the reference node, that uses the address from
     * config.conn_info.hostip and config.conn_info.hostport, then the node ip has been
     * allocated by fetchClusterConfiguration, so it must be freed. */
    if (node->ip && strcmp(node->ip, config.conn_info.hostip) != 0) sdsfree(node->ip);
    if (node->redis_config != NULL) freeServerConfig(node->redis_config);
    zfree(node->slots);
    zfree(node);
}

static void freeClusterNodes(void) {
    int i = 0;
    for (; i < config.cluster_node_count; i++) {
        clusterNode *n = config.cluster_nodes[i];
        if (n) freeClusterNode(n);
    }
    zfree(config.cluster_nodes);
    config.cluster_nodes = NULL;
}

static clusterNode **addClusterNode(clusterNode *node) {
    int count = config.cluster_node_count + 1;
    config.cluster_nodes = zrealloc(config.cluster_nodes, count * sizeof(*node));
    if (!config.cluster_nodes) return NULL;
    config.cluster_nodes[config.cluster_node_count++] = node;
    return config.cluster_nodes;
}

static int fetchClusterConfiguration(void) {
    int success = 1;
    redisContext *ctx = NULL;
    redisReply *reply = NULL;
    dict *nodes = NULL;
    const char *errmsg = "Failed to fetch cluster configuration";
    size_t i, j;
    ctx = getRedisContext(config.conn_info.hostip, config.conn_info.hostport, config.hostsocket);
    if (ctx == NULL) {
        exit(1);
    }

    reply = redisCommand(ctx, "CLUSTER SLOTS");
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        success = 0;
        if (reply) fprintf(stderr, "%s\nCLUSTER SLOTS ERROR: %s\n", errmsg, reply->str);
        goto cleanup;
    }
    assert(reply->type == REDIS_REPLY_ARRAY);
    nodes = dictCreate(&dtype);
    for (i = 0; i < reply->elements; i++) {
        redisReply *r = reply->element[i];
        assert(r->type == REDIS_REPLY_ARRAY);
        assert(r->elements >= 3);
        int from = r->element[0]->integer;
        int to = r->element[1]->integer;
        sds primary = NULL;
        for (j = 2; j < r->elements; j++) {
            redisReply *nr = r->element[j];
            assert(nr->type == REDIS_REPLY_ARRAY && nr->elements >= 3);
            assert(nr->element[0]->str != NULL);
            assert(nr->element[2]->str != NULL);

            int is_primary = (j == 2);
            if (is_primary) primary = sdsnew(nr->element[2]->str);
            int is_cluster_option_only = (config.read_from_replica == FROM_PRIMARY_ONLY);
            if ((config.read_from_replica == FROM_REPLICA_ONLY && is_primary) || (is_cluster_option_only && !is_primary)) continue;

            sds ip = sdsnew(nr->element[0]->str);
            sds name = sdsnew(nr->element[2]->str);
            int port = nr->element[1]->integer;
            int slot_start = from;
            int slot_end = to;

            clusterNode *node = NULL;
            dictEntry *entry = dictFind(nodes, name);
            if (entry == NULL) {
                node = createClusterNode(sdsnew(ip), port);
                if (node == NULL) {
                    success = 0;
                    goto cleanup;
                } else {
                    node->name = name;
                    if (!is_primary) node->replicate = sdsdup(primary);
                }
            } else {
                node = dictGetVal(entry);
            }
            if (slot_start == slot_end) {
                node->slots[node->slots_count++] = slot_start;
            } else {
                while (slot_start <= slot_end) {
                    int slot = slot_start++;
                    node->slots[node->slots_count++] = slot;
                }
            }
            if (node->slots_count == 0) {
                fprintf(stderr, "WARNING: Node %s:%d has no slots, skipping...\n", node->ip, node->port);
                continue;
            }
            if (entry == NULL) {
                dictReplace(nodes, node->name, node);
                if (!addClusterNode(node)) {
                    success = 0;
                    goto cleanup;
                }
            }
        }
        sdsfree(primary);
    }
cleanup:
    if (ctx) redisFree(ctx);
    if (!success) {
        if (config.cluster_nodes) freeClusterNodes();
    }
    if (reply) freeReplyObject(reply);
    if (nodes) dictRelease(nodes);
    return success;
}

/* Request the current cluster slots configuration by calling CLUSTER SLOTS
 * and atomically update the slots after a successful reply. */
static int fetchClusterSlotsConfiguration(client c) {
    UNUSED(c);
    int success = 1, is_fetching_slots = 0, last_update = 0;
    size_t i, j;

    last_update = atomic_load_explicit(&config.slots_last_update, memory_order_relaxed);
    if (c->slots_last_update < last_update) {
        c->slots_last_update = last_update;
        return -1;
    }
    redisReply *reply = NULL;

    is_fetching_slots = atomic_fetch_add_explicit(&config.is_fetching_slots, 1, memory_order_relaxed);
    if (is_fetching_slots) return -1; // TODO: use other codes || errno ?
    atomic_store_explicit(&config.is_fetching_slots, 1, memory_order_relaxed);
    fprintf(stderr, "WARNING: Cluster slots configuration changed, fetching new one...\n");
    const char *errmsg = "Failed to update cluster slots configuration";

    /* printf("[%d] fetchClusterSlotsConfiguration\n", c->thread_id); */
    dict *nodes = dictCreate(&dtype);
    redisContext *ctx = NULL;
    for (i = 0; i < (size_t)config.cluster_node_count; i++) {
        clusterNode *node = config.cluster_nodes[i];
        assert(node->ip != NULL);
        assert(node->name != NULL);
        assert(node->port);
        /* Use first node as entry point to connect to. */
        if (ctx == NULL) {
            ctx = getRedisContext(node->ip, node->port, NULL);
            if (!ctx) {
                success = 0;
                goto cleanup;
            }
        }
        if (node->updated_slots != NULL) zfree(node->updated_slots);
        node->updated_slots = NULL;
        node->updated_slots_count = 0;
        dictReplace(nodes, node->name, node);
    }
    reply = redisCommand(ctx, "CLUSTER SLOTS");
    if (reply == NULL || reply->type == REDIS_REPLY_ERROR) {
        success = 0;
        if (reply) fprintf(stderr, "%s\nCLUSTER SLOTS ERROR: %s\n", errmsg, reply->str);
        goto cleanup;
    }
    assert(reply->type == REDIS_REPLY_ARRAY);
    for (i = 0; i < reply->elements; i++) {
        redisReply *r = reply->element[i];
        assert(r->type == REDIS_REPLY_ARRAY);
        assert(r->elements >= 3);
        int from, to, slot;
        from = r->element[0]->integer;
        to = r->element[1]->integer;
        size_t start, end;
        if (config.read_from_replica == FROM_ALL) {
            start = 2;
            end = r->elements;
        } else if (config.read_from_replica == FROM_REPLICA_ONLY) {
            start = 3;
            end = r->elements;
        } else {
            start = 2;
            end = 3;
        }

        for (j = start; j < end; j++) {
            redisReply *nr = r->element[j];
            assert(nr->type == REDIS_REPLY_ARRAY && nr->elements >= 3);
            assert(nr->element[2]->str != NULL);
            sds name = sdsnew(nr->element[2]->str);
            dictEntry *entry = dictFind(nodes, name);
            if (entry == NULL) {
                success = 0;
                fprintf(stderr,
                        "%s: could not find node with ID %s in current "
                        "configuration.\n",
                        errmsg, name);
                if (name) sdsfree(name);
                goto cleanup;
            }
            sdsfree(name);
            clusterNode *node = dictGetVal(entry);
            if (node->updated_slots == NULL) node->updated_slots = zcalloc(CLUSTER_SLOTS * sizeof(int));
            for (slot = from; slot <= to; slot++) node->updated_slots[node->updated_slots_count++] = slot;
        }
    }
    updateClusterSlotsConfiguration();
cleanup:
    freeReplyObject(reply);
    redisFree(ctx);
    dictRelease(nodes);
    atomic_store_explicit(&config.is_fetching_slots, 0, memory_order_relaxed);
    return success;
}

/* Atomically update the new slots configuration. */
static void updateClusterSlotsConfiguration(void) {
    pthread_mutex_lock(&config.is_updating_slots_mutex);
    atomic_store_explicit(&config.is_updating_slots, 1, memory_order_relaxed);

    int i;
    for (i = 0; i < config.cluster_node_count; i++) {
        clusterNode *node = config.cluster_nodes[i];
        if (node->updated_slots != NULL) {
            int *oldslots = node->slots;
            node->slots = node->updated_slots;
            node->slots_count = node->updated_slots_count;
            node->updated_slots = NULL;
            node->updated_slots_count = 0;
            zfree(oldslots);
        }
    }
    atomic_store_explicit(&config.is_updating_slots, 0, memory_order_relaxed);
    atomic_fetch_add_explicit(&config.slots_last_update, 1, memory_order_relaxed);
    pthread_mutex_unlock(&config.is_updating_slots_mutex);
}

/* Generate random data for the benchmark. See #7196. */
static void genBenchmarkRandomData(char *data, int count) {
    static uint32_t state = 1234;
    int i = 0;

    while (count--) {
        state = (state * 1103515245 + 12345);
        data[i++] = '0' + ((state >> 16) & 63);
    }
}

long long showThroughput(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    UNUSED(eventLoop);
    UNUSED(id);
    benchmarkThread *thread = (benchmarkThread *)clientData;
    int liveclients = atomic_load_explicit(&config.liveclients, memory_order_relaxed);
    int requests_finished = atomic_load_explicit(&config.requests_finished, memory_order_relaxed);
    int previous_requests_finished = atomic_load_explicit(&config.previous_requests_finished, memory_order_relaxed);
    long long current_tick = mstime();

    if (liveclients == 0 && requests_finished != config.requests) {
        fprintf(stderr, "All clients disconnected... aborting.\n");
        exit(1);
    }
    if (config.num_threads && requests_finished >= config.requests) {
        aeStop(eventLoop);
        return AE_NOMORE;
    }
    if (config.csv) return SHOW_THROUGHPUT_INTERVAL;
    /* only first thread output throughput */
    if (thread != NULL && thread->index != 0) {
        return SHOW_THROUGHPUT_INTERVAL;
    }
    if (config.idlemode == 1) {
        printf("clients: %d\r", config.liveclients);
        fflush(stdout);
        return SHOW_THROUGHPUT_INTERVAL;
    }
    const float dt = (float)(current_tick - config.start) / 1000.0;
    const float rps = (float)requests_finished / dt;
    const float instantaneous_dt = (float)(current_tick - config.previous_tick) / 1000.0;
    const float instantaneous_rps = (float)(requests_finished - previous_requests_finished) / instantaneous_dt;
    config.previous_tick = current_tick;
    atomic_store_explicit(&config.previous_requests_finished, requests_finished, memory_order_relaxed);
    printf("%*s\r", config.last_printed_bytes, " "); /* ensure there is a clean line */
    int printed_bytes =
        printf("%s: rps=%.1f (overall: %.1f) avg_msec=%.3f (overall: %.3f)\r", config.title, instantaneous_rps, rps,
               hdr_mean(config.current_sec_latency_histogram) / 1000.0f, hdr_mean(config.latency_histogram) / 1000.0f);
    config.last_printed_bytes = printed_bytes;
    hdr_reset(config.current_sec_latency_histogram);
    fflush(stdout);
    return SHOW_THROUGHPUT_INTERVAL;
}

/* Return true if the named test was selected using the -t command line
 * switch, or if all the tests are selected (no -t passed by user). */
int test_is_selected(const char *name) {
    char buf[256];
    int l = strlen(name);

    if (config.tests == NULL) return 1;
    buf[0] = ',';
    memcpy(buf + 1, name, l);
    buf[l + 1] = ',';
    buf[l + 2] = '\0';
    return strstr(config.tests, buf) != NULL;
}

enum OPTION_CODES {
    VK_OPT_HOST = 'h',
    VK_OPT_PORT = 'p',
    VK_OPT_SOCKET = 's',
    VK_OPT_PASSWORD = 'a',
    VK_OPT_URI = 'u',
    VK_OPT_NUM_CLIENTS = 'c',
    VK_OPT_NUM_REQUESTS = 'n',
    VK_OPT_DATA_SIZE = 'd',
    VK_OPT_RESP3 = '3',
    VK_OPT_KEEPALIVE = 'k',
    VK_OPT_RANDOM_KEYS = 'r',
    VK_OPT_PIPELINE = 'P',
    VK_OPT_QUIET = 'q',
    VK_OPT_LOOP = 'l',
    VK_OPT_TESTS = 't',
    VK_OPT_IDLE = 'I',
    VK_OPT_STDIN = 'x',
    VK_OPT_VERSION = 'v',

    /* Deprecated Options */
    VK_OPT_SHOW_SERVER_ERRORS = 'e',

    VK_OPT_USER = 0x100,
    VK_OPT_DBNUM,
    VK_OPT_NUM_THREADS,
    VK_OPT_CLUSTER,
    VK_OPT_RFR,
    VK_OPT_ENABLE_TRACKING,
    VK_OPT_PRECISION,
    VK_OPT_CSV,
    VK_OPT_SEED,

#ifdef USE_OPENSSL
    VK_OPT_TLS,
    VK_OPT_SNI,
    VK_OPT_CACERT,
    VK_OPT_CACERTDIR,
    VK_OPT_INSECURE,
    VK_OPT_CERT,
    VK_OPT_KEY,
    VK_OPT_TLS_CIPHERS,
#ifdef TLS1_3_VERSION
    VK_OPT_TLS_CIPHERSUITES,
#endif
#endif

};

struct argp_option options[] = {
    {0, 0, 0, 0, "General Options:", 7},
    {0, VK_OPT_HOST, "hostname", 0, "Server hostname (default 127.0.0.1)."},
    {0, VK_OPT_PORT, "port", 0, "Server port (default 6379)."},
    {0, VK_OPT_SOCKET, "socket", 0, "Server socket (overrides host and port)."},
    {0, VK_OPT_PASSWORD, "password", 0, "Password for Valkey Auth."},
    {"user", VK_OPT_USER, "username", 0, "Used to send ACL style 'AUTH username pass'. Needs -a."},
    {0, VK_OPT_URI, "uri", 0, "Server URI on format valkey://user:password@host:port/dbnum. "
                              "User, password and dbnum are optional. For authentication "
                              "without a username, use username 'default'. For TLS, use "
                              "the scheme 'valkeys'."},
    {0, VK_OPT_NUM_CLIENTS, "clients", 0, "Number of parallel connections (default 50). "
                                          "Note: If --cluster is used then number of clients has to be "
                                          "the same or higher than the number of nodes."},
    {0, VK_OPT_NUM_REQUESTS, "requests", 0, "Total number of requests (default 100000)."},
    {0, VK_OPT_DATA_SIZE, "size", 0, "Data size of SET/GET value in bytes (default 3)."},
    {"dbnum", VK_OPT_DBNUM, "db", 0, "SELECT the specified db number (default 0)."},
    {0, VK_OPT_RESP3, 0, 0, "Start session in RESP3 protocol mode."},
    {"threads", VK_OPT_NUM_THREADS, "num", 0, "Enable multi-thread mode."},
    {"cluster", VK_OPT_CLUSTER, 0, 0, "Enable cluster mode. "
                                      "If the command is supplied on the command line in cluster "
                                      "mode, the key must contain \"{tag}\". Otherwise, the "
                                      "command will not be sent to the right cluster node."},
    {"rfr", VK_OPT_RFR, "mode", 0, "Enable read from replicas in cluster mode. "
                                   "This command must be used with the --cluster option. "
                                   "There are three modes for reading from replicas: "
                                   "'no' - sends read requests to primaries only (default); "
                                   "'yes' - sends read requests to replicas only; "
                                   "'all' - sends read requests to all nodes. "
                                   "Since write commands will not be accepted by replicas, "
                                   "it is recommended to enable read from replicas only for read command tests."},
    {"enable-tracking", VK_OPT_ENABLE_TRACKING, 0, 0, "Send CLIENT TRACKING o n before starting benchmark"},
    {0, VK_OPT_KEEPALIVE, "boolean", 0, "1=keep alive 0=reconnect (default 1)."},
    {0, VK_OPT_RANDOM_KEYS, "keyspacelen", 0, "Use random keys for SET/GET/INCR, random values for SADD, "
                                              "random members and scores for ZADD. "
                                              "Using this option the benchmark will expand the string "
                                              "__rand_int__ inside an argument with a 12 digits number in "
                                              "the specified range from 0 to keyspacelen-1. The "
                                              "substitution changes every time a command is executed. "
                                              "Default tests use this to hit random keys in the specified range. "
                                              "Note: If -r is omitted, all commands in a benchmark will "
                                              "use the same key."},
    {0, VK_OPT_PIPELINE, "numreq", 0, "Pipeline <numreq> requests. Default 1 (no pipeline)."},
    {0, VK_OPT_QUIET, 0, 0, "Quiet. Just show query/sec values."},
    {"precision", VK_OPT_PRECISION, "decimal_places", 0, "Number of decimal places to display in latency output (default 0)."},
    {"csv", VK_OPT_CSV, 0, 0, "Output in CSV format."},
    {0, VK_OPT_LOOP, 0, 0, "Loop. Run the tests forever."},
    {0, VK_OPT_TESTS, "tests", 0, "Only run the comma separated list of tests. The test "
                                  "names are the same as the ones produced as output. "
                                  "The -t option is ignored if a specific command is supplied "
                                  "on the command line."},
    {0, VK_OPT_IDLE, 0, 0, "Idle mode. Just open N idle connections and wait."},
    {0, VK_OPT_STDIN, 0, 0, "Read last argument from STDIN."},
    {"seed", VK_OPT_SEED, "num", 0, "Set the seed for random number generator. Default seed is based on time."},
    {"version", VK_OPT_VERSION, 0, 0, "Output version and exit."},
    {0, VK_OPT_SHOW_SERVER_ERRORS, 0, OPTION_HIDDEN, "[DEPRECATED] If server replies with errors, show them on stdout."},

#ifdef USE_OPENSSL
    {0, 0, 0, 0, "TLS Options:", 8},
    {"tls", VK_OPT_TLS, 0, 0, "Establish a secure TLS connection."},
    {"sni", VK_OPT_SNI, "host", 0, "Server name indication for TLS."},
    {"cacert", VK_OPT_CACERT, "file", 0, "CA Certificate file to verify with."},
    {"cacertdir", VK_OPT_CACERTDIR, "dir", 0, "Directory where trusted CA certificates are stored. "
                                              "If neither cacert nor cacertdir are specified, the default "
                                              "system-wide trusted root certs configuration will apply"},
    {"insecure", VK_OPT_INSECURE, 0, 0, "Allow insecure TLS connection by skipping cert validation."},
    {"cert", VK_OPT_CERT, "file", 0, "Client certificate to authenticate with."},
    {"key", VK_OPT_KEY, "file", 0, "Private key file to authenticate with."},
    {"tls-ciphers", VK_OPT_TLS_CIPHERS, "list", 0, "Sets the list of preferred ciphers (TLSv1.2 and below). "
                                                   "in order of preference from highest to lowest separated by colon (\":\"). "
                                                   "See the ciphers(1ssl) manpage for more information about the syntax of this string."},
#ifdef TLS1_3_VERSION
    {"tls-ciphersuites", VK_OPT_TLS_CIPHERSUITES, "list", 0, "Sets the list of preferred ciphersuites (TLSv1.3). "
                                                             "in order of preference from highest to lowest separated by colon (\":\"). "
                                                             "See the ciphers(1ssl) manpage for more information about the syntax of this string, "
                                                             "and specifically for TLSv1.3 ciphersuites."},
#endif
#endif
    {0}};

struct command {
    char *argz;
    size_t argz_len;
};

static int parse_opt(int key, char *arg, struct argp_state *state) {
    UNUSED(state);

    switch (key) {
    case VK_OPT_NUM_CLIENTS:
        config.numclients = atoi(arg);
        break;
    case VK_OPT_VERSION: {
        sds version = cliVersion();
        printf("valkey-benchmark %s\n", version);
        sdsfree(version);
        exit(0);
    }
    case VK_OPT_NUM_REQUESTS:
        config.requests = atoi(arg);
        break;
    case VK_OPT_KEEPALIVE:
        config.keepalive = atoi(arg);
        break;
    case VK_OPT_HOST:
        sdsfree(config.conn_info.hostip);
        config.conn_info.hostip = sdsnew(arg);
        break;
    case VK_OPT_PORT:
        config.conn_info.hostport = atoi(arg);
        if (config.conn_info.hostport < 0 || config.conn_info.hostport > 65535) {
            fprintf(stderr, "Invalid server port.\n");
            return -1;
        }
        break;
    case VK_OPT_SOCKET:
        config.hostsocket = strdup(arg);
        break;
    case VK_OPT_STDIN:
        config.stdinarg = 1;
        break;
    case VK_OPT_PASSWORD:
        config.conn_info.auth = sdsnew(arg);
        break;
    case VK_OPT_USER:
        config.conn_info.user = sdsnew(arg);
        break;
    case VK_OPT_URI:
        parseRedisUri(arg, "redis-benchmark", &config.conn_info, &config.tls);
        if (config.conn_info.hostport < 0 || config.conn_info.hostport > 65535) {
            fprintf(stderr, "Invalid server port.\n");
            return -1;
        }
        config.input_dbnumstr = sdsfromlonglong(config.conn_info.input_dbnum);
        break;
    case VK_OPT_RESP3:
        config.resp3 = 1;
        break;
    case VK_OPT_DATA_SIZE:
        config.datasize = atoi(arg);
        if (config.datasize < 1) config.datasize = 1;
        if (config.datasize > 1024 * 1024 * 1024) config.datasize = 1024 * 1024 * 1024;
        break;
    case VK_OPT_PIPELINE:
        config.pipeline = atoi(arg);
        if (config.pipeline <= 0) config.pipeline = 1;
        break;
    case VK_OPT_RANDOM_KEYS: {
        const char *next = arg, *p = next;
        if (*p == '-') {
            p++;
            if (*p < '0' || *p > '9') {
                fprintf(stderr, "Invalid number.\n");
                return -1;
            }
        }
        config.randomkeys = 1;
        config.randomkeys_keyspacelen = atoi(next);
        if (config.randomkeys_keyspacelen < 0) config.randomkeys_keyspacelen = 0;
        break;
    }
    case VK_OPT_QUIET:
        config.quiet = 1;
        break;
    case VK_OPT_CSV:
        config.csv = 1;
        break;
    case VK_OPT_LOOP:
        config.loop = 1;
        break;
    case VK_OPT_IDLE:
        config.idlemode = 1;
        break;
    case VK_OPT_SHOW_SERVER_ERRORS:
        fprintf(stderr, "WARNING: -e option has no effect. "
                        "We now immediately exit on error to avoid false results.\n");
        break;
    case VK_OPT_SEED: {
        int rand_seed = atoi(arg);
        srandom(rand_seed);
        init_genrand64(rand_seed);
        break;
    }
    case VK_OPT_TESTS:
        /* We get the list of tests to run as a string in the form
         * get,set,lrange,...,test_N. Then we add a comma before and
         * after the string in order to make sure that searching
         * for ",testname," will always get a match if the test is
         * enabled. */
        config.tests = sdsnew(",");
        config.tests = sdscat(config.tests, (char *)arg);
        config.tests = sdscat(config.tests, ",");
        sdstolower(config.tests);
        break;
    case VK_OPT_DBNUM:
        config.conn_info.input_dbnum = atoi(arg);
        config.input_dbnumstr = sdsfromlonglong(config.conn_info.input_dbnum);
        break;
    case VK_OPT_PRECISION:
        config.precision = atoi(arg);
        if (config.precision < 0) config.precision = DEFAULT_LATENCY_PRECISION;
        if (config.precision > MAX_LATENCY_PRECISION) config.precision = MAX_LATENCY_PRECISION;
        break;
    case VK_OPT_NUM_THREADS:
        config.num_threads = atoi(arg);
        if (config.num_threads > MAX_THREADS) {
            fprintf(stderr, "WARNING: Too many threads, limiting threads to %d.\n", MAX_THREADS);
            config.num_threads = MAX_THREADS;
        } else if (config.num_threads < 0) {
            config.num_threads = 0;
        }
        break;
    case VK_OPT_CLUSTER:
        config.cluster_mode = 1;
        break;
    case VK_OPT_RFR:
        if (!strcmp(arg, "all")) {
            config.read_from_replica = FROM_ALL;
        } else if (!strcmp(arg, "yes")) {
            config.read_from_replica = FROM_REPLICA_ONLY;
        } else if (!strcmp(arg, "no")) {
            config.read_from_replica = FROM_PRIMARY_ONLY;
        } else {
            fprintf(stderr, "Invalid value for --rfr option. Valid values are 'yes', 'no', and 'all'.\n");
            return -1;
        }
        break;
    case VK_OPT_ENABLE_TRACKING:
        config.enable_tracking = 1;
        break;
#ifdef USE_OPENSSL
    case VK_OPT_TLS:
        config.tls = 1;
        break;
    case VK_OPT_SNI:
        config.sslconfig.sni = strdup(arg);
        break;
    case VK_OPT_CACERTDIR:
        config.sslconfig.cacertdir = strdup(arg);
        break;
    case VK_OPT_CACERT:
        config.sslconfig.cacert = strdup(arg);
        break;
    case VK_OPT_INSECURE:
        config.sslconfig.skip_cert_verify = 1;
        break;
    case VK_OPT_CERT:
        config.sslconfig.cert = strdup(arg);
        break;
    case VK_OPT_KEY:
        config.sslconfig.key = strdup(arg);
        break;
    case VK_OPT_TLS_CIPHERS:
        config.sslconfig.ciphers = strdup(arg);
        break;
#ifdef TLS1_3_VERSION
    case VK_OPT_TLS_CIPHERSUITES:
        config.sslconfig.ciphersuites = strdup(arg);
        break;
#endif
#endif
    case ARGP_KEY_ARG: {
        struct command *command = (struct command *)state->input;
        error_t err = argz_add(&command->argz, &command->argz_len, arg);
        if (err == ENOMEM) {
            fprintf(stderr, "Error parsing command line argumen: Out-of-memory");
            return -err;
        }
        break;
    }
    }
    return 0;
}

int main(int argc, char **argv) {
    int i;
    char *data, *cmd, *tag;
    int len;

    setenv("ARGP_HELP_FMT", "long-opt-col=2,rmargin=100", 1);

    struct argp argp = {
        options,
        parse_opt,
        "[COMMAND ARGS...]",
        " \vExamples:\n\n"
        " Run the benchmark with the default configuration against 127.0.0.1:6379:\n"
        "   $ valkey-benchmark\n\n"
        " Use 20 parallel clients, for a total of 100k requests, against 192.168.1.1:\n"
        "   $ valkey-benchmark -h 192.168.1.1 -p 6379 -n 100000 -c 20\n\n"
        " Fill 127.0.0.1:6379 with about 1 million keys only using the SET test:\n"
        "   $ valkey-benchmark -t set -n 1000000 -r 100000000\n\n"
        " Benchmark 127.0.0.1:6379 for a few commands producing CSV output:\n"
        "   $ valkey-benchmark -t ping,set,get -n 100000 --csv\n\n"
        " Benchmark a specific command line:\n"
        "   $ valkey-benchmark -r 10000 -n 10000 eval 'return redis.call(\"ping\")' 0\n\n"
        " Fill a list with 10000 random elements:\n"
        "   $ valkey-benchmark -r 10000 -n 10000 lpush mylist __rand_int__\n\n"
        " On user specified command lines __rand_int__ is replaced with a random integer\n"
        " with a range of values selected by the -r option.\n"};

    client c;

    srandom(time(NULL) ^ getpid());
    init_genrand64(ustime() ^ getpid());
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);

    memset(&config.sslconfig, 0, sizeof(config.sslconfig));
    config.numclients = 50;
    config.requests = 100000;
    config.liveclients = 0;
    config.el = aeCreateEventLoop(1024 * 10);
    aeCreateTimeEvent(config.el, 1, showThroughput, NULL, NULL);
    config.keepalive = 1;
    config.datasize = 3;
    config.pipeline = 1;
    config.randomkeys = 0;
    config.randomkeys_keyspacelen = 0;
    config.quiet = 0;
    config.csv = 0;
    config.loop = 0;
    config.idlemode = 0;
    config.clients = listCreate();
    config.conn_info.hostip = sdsnew("127.0.0.1");
    config.conn_info.hostport = 6379;
    config.hostsocket = NULL;
    config.tests = NULL;
    config.conn_info.input_dbnum = 0;
    config.stdinarg = 0;
    config.conn_info.auth = NULL;
    config.precision = DEFAULT_LATENCY_PRECISION;
    config.num_threads = 0;
    config.threads = NULL;
    config.cluster_mode = 0;
    config.read_from_replica = FROM_PRIMARY_ONLY;
    config.cluster_node_count = 0;
    config.cluster_nodes = NULL;
    config.redis_config = NULL;
    config.is_fetching_slots = 0;
    config.is_updating_slots = 0;
    config.slots_last_update = 0;
    config.enable_tracking = 0;
    config.resp3 = 0;

    struct command command = {NULL, 0};

    int ret = argp_parse(&argp, argc, argv, 0, 0, &command);
    if (ret < 0) {
        return -ret;
    }

    tag = "";

#ifdef USE_OPENSSL
    if (config.tls) {
        cliSecureInit();
    }
#endif

    if (config.cluster_mode) {
        // We only include the slot placeholder {tag} if cluster mode is enabled
        tag = ":{tag}";

        /* Fetch cluster configuration. */
        if (!fetchClusterConfiguration() || !config.cluster_nodes) {
            if (!config.hostsocket) {
                fprintf(stderr,
                        "Failed to fetch cluster configuration from "
                        "%s:%d\n",
                        config.conn_info.hostip, config.conn_info.hostport);
            } else {
                fprintf(stderr,
                        "Failed to fetch cluster configuration from "
                        "%s\n",
                        config.hostsocket);
            }
            exit(1);
        }
        if (config.cluster_node_count == 0) {
            fprintf(stderr, "Invalid cluster: %d node(s).\n", config.cluster_node_count);
            exit(1);
        }
        const char *node_roles = NULL;
        if (config.read_from_replica == FROM_ALL) {
            node_roles = "cluster";
        } else if (config.read_from_replica == FROM_REPLICA_ONLY) {
            node_roles = "replica";
        } else {
            node_roles = "primary";
        }
        printf("Cluster has %d %s nodes:\n\n", config.cluster_node_count, node_roles);
        int i = 0;
        for (; i < config.cluster_node_count; i++) {
            clusterNode *node = config.cluster_nodes[i];
            if (!node) {
                fprintf(stderr, "Invalid cluster node #%d\n", i);
                exit(1);
            }
            const char *node_type = (node->replicate == NULL ? "Primary" : "Replica");
            printf("Node %d(%s): ", i, node_type);
            if (node->name) printf("%s ", node->name);
            printf("%s:%d\n", node->ip, node->port);
            node->redis_config = getServerConfig(node->ip, node->port, NULL);
            if (node->redis_config == NULL) {
                fprintf(stderr, "WARNING: Could not fetch node CONFIG %s:%d\n", node->ip, node->port);
            }
        }
        printf("\n");
        /* Automatically set thread number to node count if not specified
         * by the user. */
        if (config.num_threads == 0) config.num_threads = config.cluster_node_count;
    } else {
        config.redis_config = getServerConfig(config.conn_info.hostip, config.conn_info.hostport, config.hostsocket);
        if (config.redis_config == NULL) {
            fprintf(stderr, "WARNING: Could not fetch server CONFIG\n");
        }
    }
    if (config.num_threads > 0) {
        pthread_mutex_init(&(config.liveclients_mutex), NULL);
        pthread_mutex_init(&(config.is_updating_slots_mutex), NULL);
    }

    if (config.keepalive == 0) {
        fprintf(stderr, "WARNING: Keepalive disabled. You probably need "
                        "'echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse' for Linux and "
                        "'sudo sysctl -w net.inet.tcp.msl=1000' for Mac OS X in order "
                        "to use a lot of clients/requests\n");
    }
    if (command.argz != NULL && config.tests != NULL) {
        fprintf(stderr, "WARNING: Option -t is ignored.\n");
    }

    if (config.idlemode) {
        printf("Creating %d idle connections and waiting forever (Ctrl+C when done)\n", config.numclients);
        int thread_id = -1, use_threads = (config.num_threads > 0);
        if (use_threads) {
            thread_id = 0;
            initBenchmarkThreads();
        }
        c = createClient("", 0, NULL, thread_id); /* will never receive a reply */
        createMissingClients(c);
        if (use_threads)
            startBenchmarkThreads();
        else
            aeMain(config.el);
        /* and will wait for every */
    }
    if (config.csv) {
        printf("\"test\",\"rps\",\"avg_latency_ms\",\"min_latency_ms\",\"p50_latency_ms\",\"p95_latency_ms\",\"p99_"
               "latency_ms\",\"max_latency_ms\"\n");
    }
    /* Run benchmark with command in the remainder of the arguments. */
    if (command.argz != NULL) {
        if (config.stdinarg) {
            sds sin_arg = readArgFromStdin();
            argz_add(&command.argz, &command.argz_len, sin_arg);
            sdsfree(sin_arg);
        }

        size_t args_num = argz_count(command.argz, command.argz_len);
        char **args = zmalloc((args_num + 1) * sizeof(char *));
        argz_extract(command.argz, command.argz_len, args);

        /* Setup argument length */
        size_t *argvlen = zmalloc(args_num * sizeof(size_t));
        for (size_t i = 0; i < args_num; i++) argvlen[i] = strlen(args[i]);

        sds title = sdsnewlen(command.argz, command.argz_len);
        argz_stringify(title, command.argz_len, ' ');

        do {
            len = redisFormatCommandArgv(&cmd, args_num, (const char **)args, argvlen);
            // adjust the datasize to the parsed command
            config.datasize = len;
            benchmark(title, cmd, len);
            free(cmd);
        } while (config.loop);

        zfree(args);
        zfree(argvlen);
        free(command.argz);
        sdsfree(title);
        if (config.redis_config != NULL) freeServerConfig(config.redis_config);
        return 0;
    }

    /* Run default benchmark suite. */
    data = zmalloc(config.datasize + 1);
    do {
        genBenchmarkRandomData(data, config.datasize);
        data[config.datasize] = '\0';

        if (test_is_selected("ping_inline") || test_is_selected("ping")) benchmark("PING_INLINE", "PING\r\n", 6);

        if (test_is_selected("ping_mbulk") || test_is_selected("ping")) {
            len = redisFormatCommand(&cmd, "PING");
            benchmark("PING_MBULK", cmd, len);
            free(cmd);
        }

        if (test_is_selected("set")) {
            len = redisFormatCommand(&cmd, "SET key%s:__rand_int__ %s", tag, data);
            benchmark("SET", cmd, len);
            free(cmd);
        }

        if (test_is_selected("get")) {
            len = redisFormatCommand(&cmd, "GET key%s:__rand_int__", tag);
            benchmark("GET", cmd, len);
            free(cmd);
        }

        if (test_is_selected("incr")) {
            len = redisFormatCommand(&cmd, "INCR counter%s:__rand_int__", tag);
            benchmark("INCR", cmd, len);
            free(cmd);
        }

        if (test_is_selected("lpush")) {
            len = redisFormatCommand(&cmd, "LPUSH mylist%s %s", tag, data);
            benchmark("LPUSH", cmd, len);
            free(cmd);
        }

        if (test_is_selected("rpush")) {
            len = redisFormatCommand(&cmd, "RPUSH mylist%s %s", tag, data);
            benchmark("RPUSH", cmd, len);
            free(cmd);
        }

        if (test_is_selected("lpop")) {
            len = redisFormatCommand(&cmd, "LPOP mylist%s", tag);
            benchmark("LPOP", cmd, len);
            free(cmd);
        }

        if (test_is_selected("rpop")) {
            len = redisFormatCommand(&cmd, "RPOP mylist%s", tag);
            benchmark("RPOP", cmd, len);
            free(cmd);
        }

        if (test_is_selected("sadd")) {
            len = redisFormatCommand(&cmd, "SADD myset%s element:__rand_int__", tag);
            benchmark("SADD", cmd, len);
            free(cmd);
        }

        if (test_is_selected("hset")) {
            len = redisFormatCommand(&cmd, "HSET myhash%s element:__rand_int__ %s", tag, data);
            benchmark("HSET", cmd, len);
            free(cmd);
        }

        if (test_is_selected("spop")) {
            len = redisFormatCommand(&cmd, "SPOP myset%s", tag);
            benchmark("SPOP", cmd, len);
            free(cmd);
        }

        if (test_is_selected("zadd")) {
            char *score = "0";
            if (config.randomkeys) score = "__rand_int__";
            len = redisFormatCommand(&cmd, "ZADD myzset%s %s element:__rand_int__", tag, score);
            benchmark("ZADD", cmd, len);
            free(cmd);
        }

        if (test_is_selected("zpopmin")) {
            len = redisFormatCommand(&cmd, "ZPOPMIN myzset%s", tag);
            benchmark("ZPOPMIN", cmd, len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_100") || test_is_selected("lrange_300") ||
            test_is_selected("lrange_500") || test_is_selected("lrange_600")) {
            len = redisFormatCommand(&cmd, "LPUSH mylist%s %s", tag, data);
            benchmark("LPUSH (needed to benchmark LRANGE)", cmd, len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_100")) {
            len = redisFormatCommand(&cmd, "LRANGE mylist%s 0 99", tag);
            benchmark("LRANGE_100 (first 100 elements)", cmd, len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_300")) {
            len = redisFormatCommand(&cmd, "LRANGE mylist%s 0 299", tag);
            benchmark("LRANGE_300 (first 300 elements)", cmd, len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_500")) {
            len = redisFormatCommand(&cmd, "LRANGE mylist%s 0 499", tag);
            benchmark("LRANGE_500 (first 500 elements)", cmd, len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_600")) {
            len = redisFormatCommand(&cmd, "LRANGE mylist%s 0 599", tag);
            benchmark("LRANGE_600 (first 600 elements)", cmd, len);
            free(cmd);
        }

        if (test_is_selected("mset")) {
            const char *cmd_argv[21];
            cmd_argv[0] = "MSET";
            sds key_placeholder = sdscatprintf(sdsnew(""), "key%s:__rand_int__", tag);
            for (i = 1; i < 21; i += 2) {
                cmd_argv[i] = key_placeholder;
                cmd_argv[i + 1] = data;
            }
            len = redisFormatCommandArgv(&cmd, 21, cmd_argv, NULL);
            benchmark("MSET (10 keys)", cmd, len);
            free(cmd);
            sdsfree(key_placeholder);
        }

        if (test_is_selected("xadd")) {
            len = redisFormatCommand(&cmd, "XADD mystream%s * myfield %s", tag, data);
            benchmark("XADD", cmd, len);
            free(cmd);
        }

        if (!config.csv) printf("\n");
    } while (config.loop);

    zfree(data);
    freeCliConnInfo(config.conn_info);
    if (config.redis_config != NULL) freeServerConfig(config.redis_config);

    return 0;
}
