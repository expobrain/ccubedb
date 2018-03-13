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

static inline int socket_non_blocking(int fd)
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
