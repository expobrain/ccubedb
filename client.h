#ifndef CLIENT_H
#define CLIENT_H

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>

#include "sds.h"
#include "slist.h"
#include "network.h"

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


typedef enum cmd_reply cmd_reply;
enum cmd_reply {
    REPLY_OK = 0,                 /* All correct */
    REPLY_ERR_NOT_FOUND = -1,     /* Command not found */
    REPLY_ERR_WRONG_ARG = -2,     /* Command argument is wrong */
    REPLY_ERR_WRONG_ARG_NUM = -3, /* Command argument number is wrong */
    REPLY_ERR_MALFORMED_ARG = -4, /* Command argument contains non-graphic symbols*/
    REPLY_ERR_OBJ_NOT_FOUND = -5, /* Command object not found */
    REPLY_ERR_OBJ_EXISTS = -6,    /* Command object already exists */
    REPLY_ERR_ACTION_FAILED = -7, /* Command execution aborted */
};

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

static inline void client_register(client_t *client, struct sockaddr_storage *their_addr)
{
    socket_non_blocking(client->fd);

    inet_ntop(their_addr->ss_family,
              get_in_addr((struct sockaddr *)&their_addr),
              client->addrstr, sizeof(client->addrstr));

    slist_prepend(client_list, client);

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


static inline void client_sendcounter(client_t *client, counter_t counter)
{
    sds msg = sdsempty();
    msg = sdscatprintf(msg, "%zu\n", counter);
    client_add_reply(client, msg);
}

static inline void client_sendstrcnt(client_t *client, char *str, counter_t counter)
{
    sds msg = sdsempty();
    msg = sdscatprintf(msg, "%s %lu\n", str, counter);
    client_add_reply(client, msg);
}

static inline void client_sendsize(client_t *client, size_t size)
{
    sds msg = sdsempty();
    msg = sdscatprintf(msg, "%zu\n", size);
    client_add_reply(client, msg);
}

static inline void client_sendstr(client_t *client, char *str)
{
    sds msg = sdsempty();
    msg = sdscatprintf(msg, "%s\n", str);
    client_add_reply(client, msg);
}

static inline void client_sendstrstrset(client_t *client, htable_t *map)
{
    client_sendsize(client, htable_size(map));

    htable_for_each(item, map) {
        char *key = htable_key(item);
        client_sendstr(client, key);

        htable_t *set = htable_value(item);
        client_sendsize(client, htable_size(set));

        htable_for_each(set_item, set) {
            char *set_key = htable_key(set_item);
            client_sendstr(client, set_key);
        }
    }
}

static inline void client_sendstrlist(client_t *client, char **list, size_t list_size)
{
    client_sendsize(client, list_size);
    for (size_t i = 0; i < list_size; i++ )
        client_sendstr(client, list[i]);
}

static inline void client_sendstrcntmap(client_t *client, htable_t *map)
{
    client_sendsize(client, htable_size(map));

    htable_for_each(item, map) {
        char *key = htable_key(item);
        counter_t *count = htable_value(item);
        client_sendstrcnt(client, key, *count);
    }
}

static inline void client_sendstrstrcntmap(client_t *client, htable_t *map)
{
    client_sendsize(client, htable_size(map));

    htable_for_each(item, map) {
        char *key = htable_key(item);
        client_sendstr(client, key);

        htable_t *inner_map = htable_value(item);
        client_sendstrcntmap(client, inner_map);
    }
}

static inline void client_sendcode(client_t *client, cmd_reply code) {
    sds msg = sdsempty();
    msg = sdscatprintf(msg, "%d\n", code);
    client_add_reply(client, msg);
}

#endif //CLIENT_H
