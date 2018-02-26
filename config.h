#ifndef CONFIG_H
#define CONFIG_H

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

#include "cdb_alloc.h"
#include "log.h"

#define DEFAULT_PORT "1985"
#define DEFAULT_CONNECTION_NUM 64
#define DEFAULT_LOG_LEVEL LOG_DEBUG

typedef struct config_t config_t;
struct config_t {
    char *port;
    int connections;
    int log_level;
    char *log_path;
};

static inline config_t *config_create(int argc, char **argv)
{
    /* TODO: also, read from a config file first, read the args next */
    config_t *config = cdb_malloc(sizeof(*config));
    *config = (typeof(*config)) {
        .port = DEFAULT_PORT,
        .connections = DEFAULT_CONNECTION_NUM,
        .log_level = DEFAULT_LOG_LEVEL,
        .log_path = NULL
    };

    for (int i = 1; i < argc;) {
        if (0 == strcmp(argv[i], "--port")) {
            i++;
            if (argc == i) {
                fprintf(stderr,"arg error: port not supplied\n");
                exit(EXIT_FAILURE);
            }
            errno = 0;
            char *endptr = NULL;
            strtol(argv[i], &endptr, 10);
            if (0 != errno) {
                perror("strtol");
                exit(EXIT_FAILURE);
            }

            if (endptr == argv[i] || *endptr != '\0') {
                fprintf(stderr, "port value is not a number\n");
                exit(EXIT_FAILURE);
            }
            config->port = strdup(argv[i]);
        } else if (0 == strcmp(argv[i],"--log-path")) {
            i++;
            if (argc == i) {
                fprintf(stderr,"arg error: log path not supplied\n");
                exit(EXIT_FAILURE);
            }
            config->log_path = argv[i];
        } else if (0 == strcmp(argv[i],"--log-level")) {
            i++;
            if (argc == i) {
                fprintf(stderr,"arg error: log level not supplied\n");
                exit(EXIT_FAILURE);
            }
            errno = 0;
            char *endptr = NULL;
            config->log_level = strtol(argv[i], &endptr, 10);
            if (0 != errno) {
                perror("strtol");
                exit(EXIT_FAILURE);
            }
            if (endptr == argv[i] || *endptr != '\0') {
                fprintf(stderr, "log level is not a valid number\n");
                exit(EXIT_FAILURE);
            }
        }
        i++;
    }
    return config;
}

#endif //CONFIG_H
