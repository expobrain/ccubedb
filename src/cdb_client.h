#ifndef CDB_CLIENT_H
#define CDB_CLIENT_H

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>

#include "sds.h"
#include "slist.h"
#include "khash.h"
#include "network.h"

typedef struct cdb_client cdb_client;
struct cdb_client {
    int fd;
    sds querybuf;
    char addrstr[INET6_ADDRSTRLEN];
    slist_t *replies;
    size_t sentlen;
};

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

KHASH_MAP_INIT_INT(fd_to_client, cdb_client*);
extern khash_t(fd_to_client) *client_mapping;

cdb_client *cdb_client_create(int fd);

void cdb_client_mapping_init();

void cdb_client_add_reply(cdb_client *client, sds reply);

bool cdb_client_has_replies(cdb_client *client);

int cdb_client_send_replies(cdb_client *client);

cdb_client *cdb_client_find(int fd);

void cdb_client_destroy(cdb_client *client);

void cdb_client_register(cdb_client *client, struct sockaddr_storage *their_addr);

void cdb_client_unregister(int fd);

void cdb_client_sendcounter(cdb_client *client, counter_t counter);

void cdb_client_sendstrcnt(cdb_client *client, char *str, counter_t counter);

void cdb_client_sendsize(cdb_client *client, size_t size);

void cdb_client_sendstr(cdb_client *client, char *str);

void cdb_client_sendstrstrset(cdb_client *client, htable_t *map);

void cdb_client_sendstrlist(cdb_client *client, char **list, size_t list_size);

void cdb_client_sendstrcntmap(cdb_client *client, htable_t *map);

void cdb_client_sendstrstrcntmap(cdb_client *client, htable_t *map);

void cdb_client_sendcode(cdb_client *client, cmd_reply code);

#endif //CDB_CLIENT_H
