#ifndef CDB_NETWORK_H
#define CDB_NETWORK_H

#include <arpa/inet.h>
#include <assert.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>

#include "htable.h"
#include "cdb_defs.h"
#include "sds.h"
#include "cdb_config.h"

void *cdb_get_in_addr(struct sockaddr *sa);

struct addrinfo *cdb_find_bindable_addr(cdb_config *config);

int cdb_bind_addr(struct addrinfo *servinfo);

int cdb_socket_non_blocking(int fd);

#endif //CDB_NETWORK_H
