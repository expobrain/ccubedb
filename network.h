#ifndef NETWORK_H
#define NETWORK_H

#include <arpa/inet.h>
#include <assert.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>

#include "htable.h"
#include "defs.h"
#include "sds.h"
#include "config.h"
#include "client.h"

static inline void *get_in_addr(struct sockaddr *sa)
{
    if (sa->sa_family == AF_INET)
        return &(((struct sockaddr_in*)sa)->sin_addr);
    else
        return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

static inline int sendtoclient(client_t *client, sds buf)
{
    slist_prepend(client->replies, buf);
    return 0;
}

static inline int sendcounter(client_t *client, counter_t counter)
{
    sds msg = sdsempty();
    msg = sdscatprintf(msg, "%zu\n", counter);
    return sendtoclient(client, msg);
}

static inline int sendstrcnt(client_t *client, char *str, counter_t counter)
{
    sds msg = sdsempty();
    msg = sdscatprintf(msg, "%s %lu\n", str, counter);
    return sendtoclient(client, msg);
}

static inline int sendsize(client_t *client, size_t size)
{
    sds msg = sdsempty();
    msg = sdscatprintf(msg, "%zu\n", size);
    return sendtoclient(client, msg);
}

static inline int sendstr(client_t *client, char *str)
{
    sds msg = sdsempty();
    msg = sdscatprintf(msg, "%s\n", str);
    return sendtoclient(client, msg);
}

static inline int sendstrstrset(client_t *client, htable_t *map)
{
    if (sendsize(client, htable_size(map)) < 0)
        return -1;

    htable_for_each(item, map) {
        char *key = htable_key(item);
        if (sendstr(client, key) < 0)
            return -1;

        htable_t *set = htable_value(item);
        if (sendsize(client, htable_size(set)) < 0)
            return -1;

        htable_for_each(set_item, set) {
            char *set_key = htable_key(set_item);
            if (sendstr(client, set_key) < 0)
                return -1;
        }
    }

    return 0;
}

static inline int sendstrlist(client_t *client, char **list, size_t list_size)
{
    if (sendsize(client, list_size) < 0)
        return -1;

    for (size_t i = 0; i < list_size; i++ ) {
        if (sendstr(client, list[i]) < 0)
            return -1;
    }

    return 0;
}

static inline int sendstrcntmap(client_t *client, htable_t *map)
{
    if (sendsize(client, htable_size(map)) < 0)
        return -1;

    htable_for_each(item, map) {
        char *key = htable_key(item);
        counter_t *count = htable_value(item);
        if (sendstrcnt(client, key, *count) < 0)
            return -1;
    }

    return 0;
}

static inline int sendstrstrcntmap(client_t *client, htable_t *map)
{
    if (sendsize(client, htable_size(map)) < 0)
        return -1;

    htable_for_each(item, map) {
        char *key = htable_key(item);
        if (-1 == sendstr(client, key))
            return -1;

        htable_t *inner_map = htable_value(item);
        if (-1 == sendstrcntmap(client, inner_map))
            return -1;
    }

    return 0;
}


static inline int sendcode(client_t *client, int code) {
    sds msg = sdsempty();
    msg = sdscatprintf(msg, "%d\n", code);
    return sendtoclient(client, msg);
}

static inline int sendok(client_t *client)
{
    sds msg = sdsempty();
    msg = sdscatprintf(msg, "%d\n", 0);
    return sendtoclient(client, msg);
}


static inline struct addrinfo *find_bindable_addr(config_t *config)
{
    struct addrinfo *servinfo = NULL;

    /* Specify what kind of addresses we are interested in */
    struct addrinfo hints;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    /* Get the list */
    int rv = getaddrinfo(NULL, config->port, &hints, &servinfo);
    if (0 != rv) {
        log_warn("getaddrinfo: %s", gai_strerror(rv));
        exit(EXIT_FAILURE);
    }

    return servinfo;
}

static inline int bind_addr(struct addrinfo *servinfo)
{
    int sockfd;

    struct addrinfo *p;
    for(p = servinfo; p != NULL; p = p->ai_next) {
        sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (-1 == sockfd) {
            perror("server: socket");
            continue;
        }

        int yes = 1;
        if (-1 == setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes,
                             sizeof(int))) {
            perror("setsockopt");
            exit(EXIT_FAILURE);
        }

        if (-1 == bind(sockfd, p->ai_addr, p->ai_addrlen)) {
            close(sockfd);
            perror("server: bind");
            continue;
        }

        break;
    }

    if (p == NULL)  {
        log_warn("Failed to bind");
        exit(1);
    }

    return sockfd;
}

static inline int socket_non_blocking (int fd)
{

    int flags = fcntl(fd, F_GETFL, 0);
    if (1 == -flags) {
        perror("fcntl");
        return -1;
    }

    flags |= O_NONBLOCK;
    int s = fcntl(fd, F_SETFL, flags);
    if (1 == -s) {
        perror ("fcntl");
        return -1;
    }

    return 0;
}

#endif //NETWORK_H
