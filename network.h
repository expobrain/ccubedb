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

static inline void *get_in_addr(struct sockaddr *sa)
{
    if (sa->sa_family == AF_INET)
        return &(((struct sockaddr_in*)sa)->sin_addr);
    else
        return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

static inline int sendall(int conn_fd, char *buf, int len)
{
    assert(len > 0);

    int total = 0;
    int bytesleft = len;
    int n;

    while(total < len) {
        n = send(conn_fd, buf+total, (size_t)bytesleft, 0);
        if (n < 0) { break; }
        total += n;
        bytesleft -= n;
    }

    return n == -1 ? -1 : 0;
}

static inline int sendcounter(int conn_fd, counter_t counter)
{
    sds msg = sdsempty();
    msg = sdscatprintf(msg, "%zu\n", counter);
    defer { sdsfree(msg); }
    return sendall(conn_fd, msg, sdslen(msg));
}

static inline int sendstrcnt(int conn_fd, char *str, counter_t counter)
{
    sds msg = sdsempty();
    defer { sdsfree(msg); }
    msg = sdscatprintf(msg, "%s %lu\n", str, counter);
    return sendall(conn_fd, msg, sdslen(msg));
}

static inline int sendsize(int conn_fd, size_t size)
{
    sds msg = sdsempty();
    msg = sdscatprintf(msg, "%zu\n", size);
    defer { sdsfree(msg); }
    return sendall(conn_fd, msg, sdslen(msg));
}

static inline int sendstr(int conn_fd, char *str)
{
    sds msg = sdsempty();
    msg = sdscatprintf(msg, "%s\n", str);
    defer { sdsfree(msg); }
    return sendall(conn_fd, msg, sdslen(msg));
}

static inline int sendstrstrset(int conn_fd, htable_t *map)
{
    if (sendsize(conn_fd, htable_size(map)) < 0)
        return -1;

    htable_for_each(item, map) {
        char *key = htable_key(item);
        if (sendstr(conn_fd, key) < 0)
            return -1;

        htable_t *set = htable_value(item);
        if (sendsize(conn_fd, htable_size(set)) < 0)
            return -1;

        htable_for_each(set_item, set) {
            char *set_key = htable_key(set_item);
            if (sendstr(conn_fd, set_key) < 0)
                return -1;
        }
    }

    return 0;
}

static inline int sendstrlist(int conn_fd, char **list, size_t list_size)
{
    if (sendsize(conn_fd, list_size) < 0)
        return -1;

    for (size_t i = 0; i < list_size; i++ ) {
        if (sendstr(conn_fd, list[i]) < 0)
            return -1;
    }

    return 0;
}

static inline int sendstrcntmap(int conn_fd, htable_t *map)
{
    if (sendsize(conn_fd, htable_size(map)) < 0)
        return -1;

    htable_for_each(item, map) {
        char *key = htable_key(item);
        counter_t *count = htable_value(item);
        if (sendstrcnt(conn_fd, key, *count) < 0)
            return -1;
    }

    return 0;
}

static inline int sendstrstrcntmap(int conn_fd, htable_t *map)
{
    if (sendsize(conn_fd, htable_size(map)) < 0)
        return -1;

    htable_for_each(item, map) {
        char *key = htable_key(item);
        if (-1 == sendstr(conn_fd, key))
            return -1;

        htable_t *inner_map = htable_value(item);
        if (-1 == sendstrcntmap(conn_fd, inner_map))
            return -1;
    }

    return 0;
}


static inline int sendcode(int conn_fd, int code) {
    sds msg = sdsempty();
    defer { sdsfree(msg); }
    msg = sdscatprintf(msg, "%d\n", code);
    return sendall(conn_fd, msg, sdslen(msg));
}

static inline int sendok(int conn_fd)
{
    sds msg = sdsempty();
    defer { sdsfree(msg); }
    msg = sdscatprintf(msg, "%d\n", 0);
    return sendall(conn_fd, msg, sdslen(msg));
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
