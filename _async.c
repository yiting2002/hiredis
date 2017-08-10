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

#include "fmacros.h"
#include "_async.h"
#include "_ae.h"
#include <stdlib.h>
#include <mswsock.h>	/* LPFN_CONNECTEX */

static int redisAsyncHandleWrite(redisAsyncContext *ac);
static int redisAsyncHandleRead(redisAsyncContext *ac);
static void *aeCreateFileEvent(aeEventLoop *eventLoop, HANDLE fd, void *clientData);
static void aeDeleteFileEvent(aeEventLoop *eventLoop, void *eventData);

static inline SOCKET redisAsyncCreateSocket(redisAsyncContext *ac, LPFN_CONNECTEX *fn) {
    const GUID guid = WSAID_CONNECTEX;
    SOCKET s;
    DWORD bytes;
    redisNode *node, *pn;
    SOCKADDR_INET sa;

retry:
    pn = (redisNode*)&ac->nodes;
    for (;;) {
        node = pn->next;
        if (node == NULL) {
            if (ac->err == REDIS_OK)
                __redisSetError(ac,REDIS_ERR_OTHER,"No more node");
            return INVALID_SOCKET;
        }
        if (node->retry_count < 1) {
            node->retry_count++;
            if (node != ac->nodes) {
                // rotate node list
                pn->next = node->next;
                node->next = ac->nodes;
                ac->nodes = node;
            }
            break;
        }
        pn = node;
    }

    // Note, we're taking the first valid address, there may be more than one
    s = WSASocketW(node->addr.si_family, SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);
    if (s == INVALID_SOCKET) {
        __redisSetErrorFromErrno(ac,REDIS_ERR_OTHER,"Can't create socket");
        return INVALID_SOCKET;
    }

    //redisSetTcpNoDelay
    {
        int yes = 1;
        if (setsockopt(s, IPPROTO_TCP, TCP_NODELAY, (char*)&yes, sizeof(yes)) != 0) {
            __redisSetErrorFromErrno(ac,REDIS_ERR_IO,"setsockopt(TCP_NODELAY)");
            goto error;
        }
    }
    //redisSetBlocking
    /*{
        u_long yes = 1;
        if (ioctlsocket(s, FIONBIO, &yes) != 0) {
            __redisSetErrorFromErrno(ac,REDIS_ERR_IO,"ioctlsocket(FIONBIO)");
            goto error;
        }
    }*/

    // Need to bind sock before ConnectEx
    ZeroMemory(&sa, sizeof(sa));
    sa.si_family = node->addr.si_family;
    if (bind(s, (struct sockaddr*)&sa, node->addrlen) != 0) {
        __redisSetErrorFromErrno(ac,REDIS_ERR_OTHER,"Can't bind socket");
        goto error;
    }

    if (WSAIoctl(s,
        SIO_GET_EXTENSION_FUNCTION_POINTER,
        (void*)&guid,
        sizeof(GUID),
        fn,
        sizeof(LPFN_CONNECTEX),
        &bytes,
        NULL,
        NULL) != 0)
    {
        __redisSetErrorFromErrno(ac,REDIS_ERR_IO,"WSAIoctl(SIO_GET_EXTENSION_FUNCTION_POINTER)");
        goto error;
    }

    return s;
error:
    closesocket(s);
    goto retry;
}

static inline redisAsyncContext *redisAsyncContextInit(void) {
    redisAsyncContext *ac;

    ac = calloc(1,sizeof(redisAsyncContext));
    if (ac == NULL)
        return NULL;

    ac->fd = INVALID_SOCKET;
    ac->flags |= REDIS_BLOCK;

    return ac;
}

/* Helper function to free the context. */
static inline void redisAsyncContextFree(redisAsyncContext *ac) {
    redisNode *node, *pn;
    redisCallback *cb, *pc;

    ac->flags |= (REDIS_IN_CALLBACK|REDIS_DISCONNECTING|REDIS_FREEING);

    /* Execute pending callbacks with NULL reply. */
    cb = ac->replies.head;
    ac->replies.head = NULL;
    ac->replies.tail = NULL;
    while (cb != NULL) {
        if (cb->fn != NULL)
            cb->fn(ac, NULL, cb->privdata);
        pc = cb;
        cb = cb->next;
        free(pc);
    }

    if (ac->obuf != NULL)
        sdsfree(ac->obuf);
    if (ac->reader != NULL)
        redisReaderFree(ac->reader);

    /* Execute disconnect callback. When redisAsyncFree() initiated destroying
     * this context, the status will always be REDIS_OK. */
    if ((ac->flags & REDIS_CONNECTED) == 0) {
        if (ac->onConnect != NULL)
            ac->onConnect(ac,REDIS_ERR);
    } else {
        if (ac->onDisconnect != NULL)
            ac->onDisconnect(ac,(ac->err == 0) ? REDIS_OK : REDIS_ERR);
    }

    node = ac->nodes;
    while (node != NULL) {
        pn = node;
        node = node->next;
        free(pn);
    }

    free(ac);
}

int redisAsyncSlaveConnect(redisAsyncContext *ac, const char *ip, int port) {
    int rv;
    char hintp[6];
    struct addrinfo hints, *infos;
    redisNode *node, *pn;

    snprintf(hintp, sizeof(hintp), "%d", (port & 0xFFFF));	// 0-65535
    hints.ai_flags = 0;
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_addrlen = 0;
    hints.ai_canonname = NULL;
    hints.ai_addr = NULL;
    hints.ai_next = NULL;

    /* Try with IPv6 if no IPv4 address was found. We do it in this order since
     * in a Redis client you can't afford to test if you have IPv6 connectivity
     * as this would add latency to every connect. Otherwise a more sensible
     * route could be: Use IPv6 if both addresses are available and there is IPv6
     * connectivity. */
    if (getaddrinfo(ip, hintp, &hints, &infos) != 0) {
        hints.ai_family = AF_INET6;
        if ((rv = getaddrinfo(ip, hintp, &hints, &infos)) != 0) {
            __redisSetErrorFromErrcode(ac,REDIS_ERR_OTHER,rv);
            return REDIS_ERR;
        }
    }

    node = calloc(1,sizeof(redisNode));
    if (node == NULL) {
        free(node);
        __redisSetError(ac,REDIS_ERR_OOM,"Out of memory");
        return REDIS_ERR;
    }
    rv = __redisCopyAddrInfo(&node->addr, infos);
    freeaddrinfo(infos);
    if (rv == 0) {
        free(node);
        __redisSetError(ac,REDIS_ERR_OTHER,"");
        return REDIS_ERR;
    }
    node->addrlen = rv;

    pn = (redisNode*)&ac->nodes;
    while (pn->next != NULL)
        pn = pn->next;
    pn->next = node;
    return REDIS_OK;
}

redisAsyncContext* redisAsyncConnect(const char *ip, int port) {
    redisAsyncContext *ac;

    ac = redisAsyncContextInit();
    if (ac == NULL)
        return NULL;

    redisAsyncSlaveConnect(ac, ip, port);

    return ac;
}

static int __redisAsyncConnect(aeEventLoop *el, redisAsyncContext *ac) {
    LPFN_CONNECTEX fnConnectEx;
    SOCKET s;
    aeFileEvent *fe;
    redisNode *node;

retry:
    s = redisAsyncCreateSocket(ac, &fnConnectEx);
    if (s == INVALID_SOCKET)
        return REDIS_ERR;

    fe = aeCreateFileEvent(el, (HANDLE)s, ac);
    if (fe == NULL) {
        __redisSetError(ac,REDIS_ERR_OTHER,"Can't attach event");
        return REDIS_ERR;
    }

    ZeroMemory(&fe->r_ov, sizeof(fe->r_ov));
    node = ac->nodes;
    if (fnConnectEx(s, (struct sockaddr*)&node->addr, node->addrlen, NULL, 0, NULL, &fe->r_ov) == FALSE &&
        WSAGetLastError() != WSA_IO_PENDING)
    {
        __redisSetErrorFromErrno(ac,REDIS_ERR_IO,"ConnectEx()");
        aeDeleteFileEvent(el, fe);
        closesocket(s);
        goto retry;
    }

    ac->eventData = fe;
    ac->fd = s;
    ac->flags |= REDIS_BLOCK;
    fe->mask |= AEM_CONNECTING;
    __redisSetError(ac,REDIS_OK,NULL);
    return REDIS_OK;
}

/* Helper function to make the disconnect happen and clean up. */
static void __redisAsyncDisconnect(aeEventLoop *el, redisAsyncContext *ac) {
    redisCallback *cb, *pc;
    int flags;

    flags = ac->flags;
    if ((flags & (REDIS_CONNECTED|REDIS_FREEING)) != 0) {
        /* Execute pending callbacks with NULL reply. */
        cb = ac->replies.head;
        ac->replies.head = NULL;
        ac->replies.tail = NULL;
        ac->flags |= (REDIS_IN_CALLBACK|REDIS_BLOCK);
        while (cb != NULL) {
            if (cb->fn != NULL)
                cb->fn(ac, NULL, cb->privdata);
            pc = cb;
            cb = cb->next;
            free(pc);
        }
        if ((flags & REDIS_IN_CALLBACK) == 0)
            ac->flags &= ~REDIS_IN_CALLBACK;

        if (ac->obuf != NULL) {
            sdsfree(ac->obuf);
            ac->obuf = NULL;
        }
        if (ac->reader != NULL) {
            redisReaderFree(ac->reader);
            ac->reader = NULL;
        }
    }

    if (ac->eventData != NULL) {
        aeDeleteFileEvent(el, ac->eventData);
        closesocket(ac->fd);
        ac->fd = INVALID_SOCKET;
        ac->eventData = NULL;

        if ((ac->flags & REDIS_DISCONNECTING) == 0 || ac->replies.head != NULL) {
            if (__redisAsyncConnect(el, ac) == REDIS_OK)
                return;
        }
    }

    if ((flags & REDIS_IN_CALLBACK) != 0)
        return;

    /* Cleanup self */
    redisAsyncContextFree(ac);
}

/* Tries to do a clean disconnect from Redis, meaning it stops new commands
 * from being issued, but tries to flush the output buffer and execute
 * callbacks for all remaining replies. When this function is called from a
 * callback, there might be more replies and we can safely defer disconnecting
 * to redisProcessCallbacks(). Otherwise, we can only disconnect immediately
 * when there are no pending callbacks. */
void redisAsyncDisconnect(redisAsyncContext *ac) {
    ac->flags |= REDIS_DISCONNECTING;
    if ((ac->flags & REDIS_IN_CALLBACK) != 0)
        return;

    if (ac->eventData == NULL && ac->replies.head == NULL)
        __redisAsyncDisconnect(NULL, ac);
}

static inline int redisProcessCallbacks(redisAsyncContext *ac) {
    void *privdata;
    redisCallbackFn *fn;
    redisCallback *cb;
    redisReply *reply;

    while(redisReaderGetReply(ac->reader, &reply) == REDIS_OK) {
        if (reply == NULL) {
            /* When the connection is being disconnected and there are
             * no more replies, this is the cue to really disconnect. */
            if ((ac->flags & REDIS_DISCONNECTING) != 0 && ac->replies.head == NULL)
                return REDIS_ERR;

            /* When the connection is not being disconnected, simply stop
             * trying to get replies and wait for the next loop tick. */
            return REDIS_OK;
        }

        /* Even if the context is subscribed, pending regular callbacks will
         * get a reply before pub/sub messages arrive. */
        cb = ac->replies.head;
        if (reply->type == REDIS_REPLY_ERROR) {
            if (cb == NULL
                || strncmp(reply->str, "MOVED", 5) == 0
                //|| strncmp(reply->str, "ASK", 3) == 0
                //|| strncmp(reply->str, "TRYAGAIN", 8) == 0
                //|| strncmp(reply->str, "CROSSSLOT", 9) == 0
                //|| strncmp(reply->str, "CLUSTERDOWN", 11) == 0
                )
            {
                /*
                 * A spontaneous reply in a not-subscribed context can be the error
                 * reply that is sent when a new connection exceeds the maximum
                 * number of allowed connections on the server side.
                 *
                 * This is seen as an error instead of a regular reply because the
                 * server closes the connection after sending it.
                 *
                 * To prevent the error from being overwritten by an EOF error the
                 * connection is closed here. See issue #43.
                 *
                 * Another possibility is that the server is loading its dataset.
                 * In this case we also want to close the connection, and have the
                 * user wait until the server is ready to take our request.
                 */
                ac->nodes->retry_count++;
                ac->err = REDIS_ERR_OTHER;
                snprintf(ac->errstr, sizeof(ac->errstr), "%s", reply->str);
                freeReplyObject(reply);
                return REDIS_ERR;
            }
        }
        if (cb != NULL) {
            fn = cb->fn;
            privdata = cb->privdata;
            if ((ac->replies.head = cb->next) == NULL)
                ac->replies.tail = NULL;
            free(cb);

            if (fn != NULL) {
                ac->flags |= REDIS_IN_CALLBACK;
                fn(ac, reply, privdata);
                ac->flags &= ~REDIS_IN_CALLBACK;
            }
        }

        /* No callback for this reply. This can either be a NULL callback,
         * or there were no callbacks to begin with. Either way, don't
         * abort with an error, but simply ignore it because the client
         * doesn't know what the server will spit out over the wire. */
        freeReplyObject(reply);
    }

    /* Disconnect when there was an error reading the reply */
    __redisSetError(ac,ac->reader->err,ac->reader->errstr);
    return REDIS_ERR;
}

/* Internal helper function to detect socket status the first time a read or
 * write event fires. When connecting was not successful, the connect callback
 * is called with a REDIS_ERR status and the context is free'd. */
static inline int redisAsyncHandleConnect(redisAsyncContext *ac) {
    DWORD bytes, flags;
    redisNode *node;
    aeFileEvent *fe;

    /*if (getsockopt(c->fd, SOL_SOCKET, SO_ERROR, (char*)&err, &errlen) == -1) {
        __redisSetErrorFromErrno(c,REDIS_ERR_IO,"getsockopt(SO_ERROR)");
        return REDIS_ERR;
    }
    if (err != 0) {
        __redisSetErrorFromErrcode(c,REDIS_ERR_IO,err);
        return REDIS_ERR;
    }*/

    fe = ac->eventData;
    fe->mask &= ~AEM_CONNECTING;
    if (WSAGetOverlappedResult(ac->fd, &fe->r_ov, &bytes, FALSE, &flags) == FALSE) {
        __redisSetErrorFromErrno(ac,REDIS_ERR_IO,"WSAGetOverlappedResult(ConnectEx)");
        return REDIS_ERR;
    }

    ac->reader = redisReaderCreate();
    if (ac->reader == NULL) {
        __redisSetError(ac,REDIS_ERR_OOM,"Out of memory");
        return REDIS_ERR;
    }
    if (redisAsyncHandleRead(ac) != REDIS_OK)
        return REDIS_ERR;

    /* Mark context as connected. */
    if ((ac->flags & REDIS_CONNECTED) == 0) {
        ac->flags |= REDIS_CONNECTED;
        if (ac->onConnect != NULL) {
            ac->flags |= REDIS_IN_CALLBACK;
            ac->onConnect(ac, REDIS_OK);
            ac->flags &= ~REDIS_IN_CALLBACK;

            if (ac->eventData == NULL)
                return REDIS_ERR;
        }
    }

    if (redisAsyncHandleWrite(ac) != REDIS_OK)
        return REDIS_ERR;

    /* reset retry counter */
    node = ac->nodes;
    while (node != NULL) {
        node->retry_count = 0;
        node = node->next;
    }

    return REDIS_OK;
}

/* Use this function to handle a read event on the descriptor. It will try
 * and read some bytes from the socket and feed them to the reply parser.
 *
 * After this function is called, you may use redisContextReadReply to
 * see if there is a reply available. */
static int redisAsyncHandleRead(redisAsyncContext *ac) {
    DWORD bytes, flags;
    WSABUF wbuf;
    aeFileEvent *fe;
    char *buf;

    fe = ac->eventData;
    buf = fe->r_buf;
    if (buf == NULL) {
        buf = malloc(REDIS_READER_MAX_BUF);
        if (buf == NULL) {
            __redisSetError(ac,REDIS_ERR_OOM,"Out of memory");
            return REDIS_ERR;
        }
        fe->r_buf = buf;
    } else {
        /* process read result */
        if (WSAGetOverlappedResult(ac->fd, &fe->r_ov, &bytes, FALSE, &flags) == FALSE) {
            __redisSetErrorFromErrno(ac,REDIS_ERR_IO,"WSAGetOverlappedResult(r_ov)");
            goto error;
        }
        if (bytes == 0) {
            __redisSetError(ac,REDIS_ERR_IO,"WSARecv() partial");
            goto error;
        }
        if (redisReaderFeed(ac->reader, buf, bytes) != REDIS_OK) {
            __redisSetError(ac,ac->reader->err,ac->reader->errstr);
            goto error;
        }
    }

    /* Return early when the context has seen an error. */
    if (ac->err != 0)
        goto error;
    if ((ac->flags & REDIS_DISCONNECTING) != 0 && ac->replies.head == NULL)
        goto error;

    /* queue read event */
    ZeroMemory(&fe->r_ov, sizeof(fe->r_ov));
    flags = 0;
    wbuf.buf = buf;
    wbuf.len = REDIS_READER_MAX_BUF;
    if (WSARecv(ac->fd, &wbuf, 1, &bytes, &flags, &fe->r_ov, NULL) != 0 &&
        WSAGetLastError() != WSA_IO_PENDING)
    {
        __redisSetErrorFromErrno(ac,REDIS_ERR_IO,"WSARecv()");
        goto error;
    }

    return REDIS_OK;
error:
    /* free read event */
    fe->r_buf = NULL;
    free(buf);
    return REDIS_ERR;
}

/* Write the output buffer to the socket.
 *
 * Returns REDIS_OK when the buffer is empty, or (a part of) the buffer was
 * successfully written to the socket. When the buffer is empty after the
 * write operation, "done" is set to 1 (if given).
 *
 * Returns REDIS_ERR if an error occurred trying to write and sets
 * c->errstr to hold the appropriate error string.
 */
static int redisAsyncHandleWrite(redisAsyncContext *ac) {
    DWORD bytes, flags;
    WSABUF wbuf;
    aeFileEvent *fe;
    char *buf;

    fe = ac->eventData;
    buf = fe->w_buf;
    if (buf != NULL) {
        /* process write result */
        if (WSAGetOverlappedResult(ac->fd, &fe->w_ov, &bytes, FALSE, &flags) == FALSE) {
            __redisSetErrorFromErrno(ac,REDIS_ERR_IO,"WSAGetOverlappedResult(w_ov)");
            goto error;
        }
        if (bytes != sdslen(buf)) {
            __redisSetError(ac,REDIS_ERR_IO,"WSASend() partial");
            goto error;
        }
        sdsfree(buf);
        fe->w_buf = NULL;
        buf = NULL;
    }

    /* Return early when the context has seen an error. */
    if (ac->err != 0)
        goto error;
    if ((ac->flags & REDIS_DISCONNECTING) != 0 && ac->replies.head == NULL)
        goto error;

    buf = ac->obuf;
    if (buf != NULL) {
        ac->obuf = NULL;
        fe->w_buf = buf;

        /* queue write event */
        ZeroMemory(&fe->w_ov, sizeof(fe->w_ov));
        wbuf.buf = buf;
        wbuf.len = sdslen(buf);
        if (WSASend(ac->fd, &wbuf, 1, &bytes, 0, &fe->w_ov, NULL) != 0 &&
            WSAGetLastError() != WSA_IO_PENDING)
        {
            __redisSetErrorFromErrno(ac,REDIS_ERR_IO,"WSASend()");
            goto error;
        }
        ac->flags |= REDIS_BLOCK;
    } else {
        ac->flags &= ~REDIS_BLOCK;
    }

    return REDIS_OK;
error:
    /* free write event */
    if (buf != NULL) {
        fe->w_buf = NULL;
        sdsfree(buf);
    }
    return REDIS_ERR;
}

/* Helper function for the redisAsyncCommand* family of functions. Writes a
 * formatted command to the output buffer and registers the provided callback
 * function with the context. */
static int __redisAsyncCommand(redisAsyncContext *ac, redisCallbackFn *fn, void *privdata) {
    aeFileEvent *fe;
    redisCallback *cb;

    /* Setup callback */
    cb = malloc(sizeof(redisCallback));
    if (cb == NULL) {
        __redisSetError(ac,REDIS_ERR_OOM,"Out of memory");
        return REDIS_ERR;
    }
    cb->next = NULL;
    cb->fn = fn;
    cb->privdata = privdata;

    /* Setup callback */
    if (ac->replies.tail != NULL)
        ac->replies.tail->next = cb;
    else
        ac->replies.head = cb;
    ac->replies.tail = cb;

    /* Always schedule a write when the write buffer is non-empty */
    if ((ac->flags & REDIS_BLOCK) == 0) {
        ac->flags |= REDIS_BLOCK;
        fe = ac->eventData;
        if (PostQueuedCompletionStatus(fe->iocp,0,(ULONG_PTR)fe,&fe->w_ov) == FALSE) {
            __redisSetError(ac,REDIS_ERR_IO,"PostQueuedCompletionStatus()");
            return REDIS_ERR;
        }
    }

    return REDIS_OK;
}

int redisAsyncCommandArgv(redisAsyncContext *ac, redisCallbackFn *fn, void *privdata, int argc, const char **argv) {
    /* Don't accept new commands when the connection is about to be closed. */
    if ((ac->flags & REDIS_DISCONNECTING) != 0)
        return REDIS_ERR;

    if (redisFormatSdsCommandArgv(&ac->obuf, argc, argv) < 0) {
        __redisSetError(ac,REDIS_ERR_OOM,"Out of memory");
        return REDIS_ERR;
    }

    return __redisAsyncCommand(ac, fn, privdata);
}

int redisAsyncFormattedCommand(redisAsyncContext *ac, redisCallbackFn *fn, void *privdata, const char *cmd) {
    sds newbuf;

    /* Don't accept new commands when the connection is about to be closed. */
    if ((ac->flags & REDIS_DISCONNECTING) != 0)
        return REDIS_ERR;

    newbuf = ac->obuf;
    if (newbuf == NULL)
        newbuf = sdsnew(cmd);
    else
        newbuf = sdscat(newbuf, cmd);
    if (newbuf == NULL) {
        __redisSetError(ac,REDIS_ERR_OOM,"Out of memory");
        return REDIS_ERR;
    }
    ac->obuf = newbuf;

    return __redisAsyncCommand(ac, fn, privdata);
}

int redisAeAttach(aeEventLoop *el, redisAsyncContext *ac, redisConnectCallback *cfn, redisDisconnectCallback *dfn) {
    /* Nothing should be attached when something is already attached */
    if (ac->eventData != NULL)
        return REDIS_ERR;
    if (ac->nodes == NULL)
        return REDIS_ERR;

    ac->onConnect = cfn;
    ac->onDisconnect = dfn;

    if (__redisAsyncConnect(el, ac) != REDIS_OK)
        return REDIS_ERR;

    return REDIS_OK;
}

void redisAeDetach(aeEventLoop *el, redisAsyncContext *ac) {
    if (ac->eventData == NULL)
        return;

    ac->onConnect = NULL;
    ac->onDisconnect = NULL;

    ac->flags |= (REDIS_DISCONNECTING|REDIS_FREEING);
    __redisAsyncDisconnect(el, ac);
}
