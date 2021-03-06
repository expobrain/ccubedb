#ifndef CDB_CONFIG_H
#define CDB_CONFIG_H

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>

#include "cdb_alloc.h"
#include "cdb_log.h"

#define DEFAULT_PORT "1985"
#define DEFAULT_CONNECTION_NUM 64
#define DEFAULT_LOG_LEVEL LOG_DEBUG

typedef struct cdb_config {
    char *port;
    int connection_num;

    int log_level;
    char *log_path;

    char *dump_path;
} cdb_config;

cdb_config *cdb_config_create(int argc, char **argv);

#endif //CDB_CONFIG_H
