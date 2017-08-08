/*
 * Copyright (c) 2017, Yiting <yiting2002 at gmail dot com>
 *
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

#ifndef __HIREDIS_ASYNC_H
#define __HIREDIS_ASYNC_H
#include "hiredis.h"

#ifdef __cplusplus
extern "C" {
#endif

struct redisAsyncContext; /* need forward declaration of redisAsyncContext */
struct aeEventLoop;

/* Reply callback prototype and container */
typedef void (redisCallbackFn)(struct redisAsyncContext*, void*, void*);
typedef struct redisCallback {
    struct redisCallback *next; /* simple singly linked list */
    redisCallbackFn *fn;
    void *privdata;
} redisCallback;

/* List of callbacks for either regular replies or pub/sub */
typedef struct redisCallbackList {
    redisCallback *head, *tail;
} redisCallbackList;

/* Server node data */
typedef struct redisNode {
    struct redisNode *next;
    int retry_count;
    int addrlen;
    SOCKADDR_INET addr;
} redisNode;

/* Connection callback prototypes */
typedef void (redisDisconnectCallback)(const struct redisAsyncContext*, int status);
typedef void (redisConnectCallback)(const struct redisAsyncContext*, int status);

/* Context for an async connection to Redis */
typedef struct redisAsyncContext {
    int err; /* Error flags, 0 when there is no error */
    char errstr[128]; /* String representation of error when applicable */
    SOCKET fd;
    int flags;
    sds obuf; /* Write buffer */
    redisReader *reader; /* Protocol reader */

    /* Server list, the first node is master */
    redisNode *nodes;

    /* Event library data */
    void *eventData;
    void *clusterData;

    /* Called when either the connection is terminated due to an error or per
     * user request. The status is set accordingly (REDIS_OK, REDIS_ERR). */
    redisDisconnectCallback *onDisconnect;

    /* Called when the first write event was received. */
    redisConnectCallback *onConnect;

    /* Regular command callbacks */
    redisCallbackList replies;
} redisAsyncContext;

/* Functions that proxy to hiredis */
redisAsyncContext *redisAsyncConnect(const char *ip, int port);
int redisAsyncSlaveConnect(redisAsyncContext *ac, const char *ip, int port);
void redisAsyncDisconnect(redisAsyncContext *ac);

/* Command functions for an async context. Write the command to the
 * output buffer and register the provided callback. */
int redisAsyncCommandArgv(redisAsyncContext *ac, redisCallbackFn *fn, void *privdata, int argc, const char **argv);
int redisAsyncFormattedCommand(redisAsyncContext *ac, redisCallbackFn *fn, void *privdata, const char *cmd);

/* Event functions */
int redisAeAttach(struct aeEventLoop *el, redisAsyncContext *ac, redisConnectCallback *cfn, redisDisconnectCallback *dfn);
void redisAeDetach(struct aeEventLoop *el, redisAsyncContext *ac);

#ifdef __cplusplus
}
#endif

#endif
