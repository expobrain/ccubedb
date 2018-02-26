#ifndef CLIENT_H
#define CLIENT_H

#include <arpa/inet.h>

#include "sds.h"
#include "slist.h"

extern slist_t *client_list;

typedef struct client_t client_t;
struct client_t {
    int fd;
    sds querybuf;
    char addrstr[INET6_ADDRSTRLEN];
};

static inline client_t *client_create(int fd)
{
    client_t *client = cdb_malloc(sizeof(*client));
    *client = (typeof(*client)) {
        .fd = fd,
        .querybuf = sdsempty(),
        .addrstr = ""
    };
    return client;
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

static inline client_t *client_delete(int fd)
{
    bool finder(void *data) {
        client_t *client = data;
        return client->fd == fd;
    }
    return slist_delete_single_if(client_list, finder);
}

static inline void client_destroy(client_t *client)
{
    sdsfree(client->querybuf);
    free(client);
}

#endif //CLIENT_H
