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

/* Return the number of digits of 'v' when converted to string in radix 10.
 * Implementation borrowed from link in redis/src/util.c:string2ll(). */
static inline int countDigits(size_t v) {
    int result = 1;
    for (;;) {
        if (v < 10) return result;
        if (v < 100) return result + 1;
        if (v < 1000) return result + 2;
        if (v < 10000) return result + 3;
        v /= 10000U;
        result += 4;
    }
}

/* Helper that calculates the bulk length given a certain string length. */
static inline int bulklen(size_t len) {
    return 1+countDigits(len)+2+len+2;
}

/* Format a command according to the Redis protocol using an sds string and
 * sdscatfmt for the processing of arguments. This function takes the
 * number of arguments, an array with arguments and an array with their
 * lengths. If the latter is set to NULL, strlen will be used to compute the
 * argument lengths.
 */
static inline int redisFormatSdsCommandArgv(sds *target, int argc, const char **argv) {
    sds cmd;
    int totlen, len;
    int j;
    int argvlen[8];

    /* Calculate our total size */
    totlen = 1+countDigits(argc)+2;
    for (j = 0; j < argc; j++) {
        len = strlen(argv[j]);
        if (j < 8) argvlen[j] = len;
        totlen += bulklen(len);
    }

    /* We already know how much storage we need */
    cmd = *target;
    if (cmd == NULL)
        cmd = sdsnewlen(NULL, totlen);
    else
        cmd = sdsMakeRoomFor(cmd, totlen);
    if (cmd == NULL)
        return -1;

    /* Construct command */
    sdscatfmt(cmd, "*%i\r\n", argc);
    for (j = 0; j < argc; j++) {
        len = (j < 8) ? argvlen[j] : strlen(argv[j]);
        sdscatfmt(cmd, "$%u\r\n", len);
        sdscatlen(cmd, argv[j], len);
        sdscatlen(cmd, "\r\n", 2);
    }

    //assert(sdslen(cmd)==totlen);

    *target = cmd;
    return totlen;
}

static void __redisSetError(void *ctx, int type, const char *str) {
    size_t len;
    redisContext *c = ctx;

    c->err = type;
    if (str != NULL) {
        len = strlen(str);
        len = len < (sizeof(c->errstr)-1) ? len : (sizeof(c->errstr)-1);
        CopyMemory(c->errstr,str,len);
        c->errstr[len] = '\0';
    }
    else {
        c->errstr[0] = '\0';
    }
}

static void __redisSetErrorFromErrno(void *ctx, int type, const char *prefix) {
    char buf[128];
    DWORD len = 0;
    DWORD errorno = WSAGetLastError();  /* snprintf() may change errno */

    if (prefix != NULL)
        len = snprintf(buf,sizeof(buf),"%s: ",prefix);
    /* gai_strerrorA() */
    len += FormatMessageA(
        FORMAT_MESSAGE_FROM_SYSTEM
        | FORMAT_MESSAGE_IGNORE_INSERTS
        | FORMAT_MESSAGE_MAX_WIDTH_MASK,
        NULL,
        errorno,
        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        buf + len,
        sizeof(buf) - len,
        NULL);
    buf[len] = 0;
    __redisSetError(ctx,type,buf);
}

static void __redisSetErrorFromErrcode(void *ctx, int type, DWORD errorno) {
    char buf[128];
    DWORD len;

    /* gai_strerrorA() */
    len = FormatMessageA(
        FORMAT_MESSAGE_FROM_SYSTEM
        | FORMAT_MESSAGE_IGNORE_INSERTS
        | FORMAT_MESSAGE_MAX_WIDTH_MASK,
        NULL,
        errorno,
        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        buf,
        sizeof(buf),
        NULL);
    buf[len] = 0;
    __redisSetError(ctx,type,buf);
}

static inline int __redisCopyAddrInfo(SOCKADDR_INET *addr, const struct addrinfo *info) {
    while (info != NULL) {
        if ((info->ai_family == AF_INET || info->ai_family == AF_INET6) &&
            info->ai_addrlen <= sizeof(SOCKADDR_INET))
        {
            CopyMemory(addr,info->ai_addr,info->ai_addrlen);
            return info->ai_addrlen;
        }
        info = info->ai_next;
    }
    return 0;
}
