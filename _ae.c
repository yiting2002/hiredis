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
#include "_ae.h"
#include <stdlib.h>

static inline void redisAeConnectEvent(aeEventLoop *el, void *privdata) {
    redisAsyncContext *ac = privdata;

    if (redisAsyncHandleConnect(ac) != REDIS_OK)
        goto error;

    return;
error:
    __redisAsyncDisconnect(el, ac);
}

static inline void redisAeWriteEvent(aeEventLoop *el, void *privdata) {
    redisAsyncContext *ac = privdata;

    if (redisAsyncHandleWrite(ac) != REDIS_OK)
        goto error;

    return;
error:
    __redisAsyncDisconnect(el, ac);
}

static inline void redisAeReadEvent(aeEventLoop *el, void *privdata) {
    redisAsyncContext *ac = privdata;

    if (redisAsyncHandleRead(ac) != REDIS_OK)
        goto error;
    if (redisProcessCallbacks(ac) != REDIS_OK)
        goto error;

    return;
error:
    __redisAsyncDisconnect(el, ac);
}

aeEventLoop *aeCreateEventLoop(int setsize) {
    int i;
    HANDLE iocp;
    aeEventLoop *eventLoop;

    eventLoop = calloc(1, sizeof(aeEventLoop));
    if (eventLoop == NULL) goto err0;
    eventLoop->events = calloc(setsize, sizeof(aeFileEvent));
    if (eventLoop->events == NULL) goto err1;
    iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 1);
    if (iocp == NULL) goto err2;
    eventLoop->iocp = iocp;
    eventLoop->setsize = setsize;
    eventLoop->maxfd = -1;

    /* Events with mask == AE_NONE are not set. So let's initialize the
    * vector with it. */
    for (i = 0; i < setsize; i++) {
        eventLoop->events[i].mask = AEM_NONE;
        eventLoop->events[i].iocp = iocp;
    }
    return eventLoop;

err2:
    free(eventLoop->events);
err1:
    free(eventLoop);
err0:
    return NULL;
}

void aeDeleteEventLoop(aeEventLoop *eventLoop) {
    CloseHandle(eventLoop->iocp);
    free(eventLoop->events);
    free(eventLoop);
}

static void *aeCreateFileEvent(aeEventLoop *eventLoop, HANDLE fd, void *clientData) {
    int i, size;
    aeFileEvent *fe;

    size = eventLoop->setsize;
    for (i = 0; i < size; i++) {
        if (eventLoop->events[i].mask == AEM_NONE) {
            fe = &eventLoop->events[i];
            if (CreateIoCompletionPort(fd, eventLoop->iocp, (ULONG_PTR)fe, 0) != NULL) {
                fe->mask = AEM_ATTACHED;
                fe->clientData = clientData;
                if (eventLoop->maxfd < i)
                    eventLoop->maxfd = i;
                return fe;
            }
            break;
        }
    }

    return NULL;
}

static void aeDeleteFileEvent(aeEventLoop *eventLoop, void *eventData) {
    aeFileEvent *fe = eventData;

    if ((fe->mask & AEM_CONNECTING) == 0 && fe->r_buf == NULL && fe->w_buf == NULL) {
        fe->mask = AEM_NONE;
        if (eventLoop->events + eventLoop->maxfd == fe) {
            int i = eventLoop->maxfd;
            while ((--i) >= 0 && (--fe)->mask == AEM_NONE);
            eventLoop->maxfd = i;
        }
    }
    else
        fe->mask = AEM_CLOSING;
    fe->clientData = NULL;
}

/* Process every pending time event, then every pending file event
 * (that may be registered by time event callbacks just processed).
 * Without special flags the function sleeps until some file event
 * fires, or when the next time event occurs (if any).
 *
 * If flags is 0, the function does nothing and returns.
 * if flags has AE_ALL_EVENTS set, all the kind of events are processed.
 * if flags has AE_FILE_EVENTS set, file events are processed.
 * if flags has AE_TIME_EVENTS set, time events are processed.
 * if flags has AE_DONT_WAIT set the function returns ASAP until all
 * if flags has AE_CALL_AFTER_SLEEP set, the aftersleep callback is called.
 * the events that's possible to process without to wait are processed.
 *
 * The function returns the number of events processed. */
int aeProcessEvents(aeEventLoop *eventLoop, int flags) {
    DWORD bytes;
    ULONG_PTR key;
    LPOVERLAPPED ov;
    aeFileEvent *fe;
    DWORD timeout;

    timeout = (flags & AE_DONT_WAIT) == 0 ? INFINITE : 0;

    for (;;) {
        if (eventLoop->maxfd < 0)
            return -1;
        if (GetQueuedCompletionStatus(eventLoop->iocp, &bytes, &key, &ov, timeout) == FALSE && ov == NULL)
            break;

        fe = (aeFileEvent*)key;
        if ((fe->mask & AEM_ATTACHED) != 0) {
            if (ov == &fe->r_ov) {
                if (fe->r_buf == NULL)
                    redisAeConnectEvent(eventLoop,fe->clientData);
                else
                    redisAeReadEvent(eventLoop,fe->clientData);
            }
            else if (ov == &fe->w_ov) {
                redisAeWriteEvent(eventLoop,fe->clientData);
            }
        } else {
            if (ov == &fe->r_ov) {
                if (fe->r_buf != NULL) {
                    free(fe->r_buf);
                    fe->r_buf = NULL;
                }
            }
            else if (ov == &fe->w_ov) {
                if (fe->w_buf != NULL) {
                    sdsfree(fe->w_buf);
                    fe->w_buf = NULL;
                }
            }
            if (fe->r_buf == NULL && fe->w_buf == NULL)
                aeDeleteFileEvent(eventLoop, fe);
        }
    }

    return 0;
}
