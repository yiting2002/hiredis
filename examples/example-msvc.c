#include "../fmacros.h"
#include "../hiredis.h"
#include "../_async.h"
#include "../_ae.h"

void getCallback(redisAsyncContext *c, void *r, void *privdata) {
    redisReply *reply = r;
    if (reply == NULL) return;

    printf("[%s]: %s\n", (char*)privdata, reply->str);
}

void connectCallback(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        printf("Connect Error: %s\n", c->errstr);
        return;
    }

    printf("Connected...\n");
}

void disconnectCallback(const redisAsyncContext *c, int status) {
    if (status != REDIS_OK) {
        printf("Disconnect Error: %s\n", c->errstr);
        return;
    }

    printf("Disconnected...\n");
}

int main(void) {
    aeEventLoop *loop;
    redisAsyncContext *ctx;
    WSADATA wsa;

    WSAStartup(MAKEWORD(2, 2), &wsa);

    loop = aeCreateEventLoop(8);

    ctx = redisAsyncConnect("127.0.0.1", 6379);
    redisAsyncFormattedCommand(ctx, getCallback, "PING", "*1\r\n$4\r\nPING\r\n");
    redisAsyncDisconnect(ctx);

    redisAeAttach(loop, ctx, connectCallback, disconnectCallback);
    //redisAeDetach(loop, ctx);

    aeProcessEvents(loop, 0);
    aeDeleteEventLoop(loop);

    WSACleanup();
    return 0;
}
