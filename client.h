#ifndef CLIENT_H
#define CLIENT_H

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>

#include "sds.h"
#include "slist.h"

extern slist_t *client_list;

typedef struct client_t client_t;
struct client_t {
    int fd;
    sds querybuf;
    char addrstr[INET6_ADDRSTRLEN];
    slist_t *replies;
    size_t sentlen;
};

static inline client_t *client_create(int fd)
{
    client_t *client = cdb_malloc(sizeof(*client));
    *client = (typeof(*client)) {
        .fd = fd,
        .querybuf = sdsempty(),
        .addrstr = "",
        .replies = slist_create(),
        .sentlen = 0
    };
    return client;
}

static inline void client_add_reply(client_t *client, sds reply)
{
    slist_append(client->replies, reply);
}

static inline bool client_has_replies(client_t *client)
{
    return slist_size(client->replies);
}

static inline int client_send_replies(client_t *client)
{
    int bytes_sent = 0;
    int replies_sent = 0;
    while(client_has_replies(client)) {
        sds reply = slist_head(client->replies);
        size_t replylen = sdslen(reply);

        /* Skip empty bufs */
        if (!replylen) {
            slist_pop_head(client->replies);
            continue;
        }

        bytes_sent = send(client->fd, reply + client->sentlen, replylen - client->sentlen, 0);

        /* Either something went wrong, or we'll have to proceed on the next event loop cycle */
        if (bytes_sent <= 0) break;

        client->sentlen += (size_t)bytes_sent;

        if (client->sentlen == replylen){
            client->sentlen = 0;
            sdsfree(reply);
            slist_pop_head(client->replies);
            replies_sent++;
        }
    }

    if (bytes_sent == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            /* Cannot write yet, come back later */
            bytes_sent = 0;
        } else {
            /* Failed to write to the client. Error? */
            return -1;
        }
    }

    return replies_sent;
}

static inline client_t *client_find(int fd)
{
    slist_for_each(node, client_list) {
        client_t *client = slist_data(node);
        if (fd == client->fd)
            return client;
    }
    return NULL;
}

static inline void client_destroy(client_t *client)
{
    slist_for_each(node, client->replies)
        sdsfree(slist_data(node));
    slist_destroy(client->replies);
    sdsfree(client->querybuf);
    free(client);
}

static inline void client_delete(int fd)
{
    bool finder(void *data) {
        client_t *client = data;
        return client->fd == fd;
    }
    close(fd);
    client_t *client = slist_delete_single_if(client_list, finder);
    client_destroy(client);
}

#endif //CLIENT_H
