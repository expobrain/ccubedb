#include "cdb_client.h"


cdb_client *cdb_client_create(int fd)
{
    cdb_client *client = cdb_malloc(sizeof(*client));
    *client = (typeof(*client)) {
        .fd = fd,
        .querybuf = sdsempty(),
        .addrstr = "",
        .replies = slist_create(),
        .sentlen = 0
    };
    return client;
}

void cdb_client_mapping_init()
{
    client_mapping = kh_init(fd_to_client);
}

void cdb_client_add_reply(cdb_client *client, sds reply)
{
    slist_append(client->replies, reply);
}

bool cdb_client_has_replies(cdb_client *client)
{
    return slist_size(client->replies);
}

int cdb_client_send_replies(cdb_client *client)
{
    int bytes_sent = 0;
    int replies_sent = 0;
    while(cdb_client_has_replies(client)) {
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

cdb_client *cdb_client_find(int fd)
{
    khint_t key = kh_get(fd_to_client, client_mapping, (khint_t)fd);
    bool is_found = key != kh_end(client_mapping);
    if (!is_found)
        return NULL;
    return kh_value(client_mapping, key);
}

void cdb_client_destroy(cdb_client *client)
{
    slist_for_each(node, client->replies)
        sdsfree(slist_data(node));
    slist_destroy(client->replies);
    sdsfree(client->querybuf);
    free(client);
}

void cdb_client_register(cdb_client *client, struct sockaddr_storage *their_addr)
{
    cdb_socket_non_blocking(client->fd);

    inet_ntop(their_addr->ss_family,
              cdb_get_in_addr((struct sockaddr *)&their_addr),
              client->addrstr, sizeof(client->addrstr));

    int ret;
    khint_t key = kh_put(fd_to_client, client_mapping, (khint_t)client->fd, &ret);
    kh_value(client_mapping, key) = client;
}

void cdb_client_unregister(int fd)
{
    close(fd);

    khint_t key = kh_get(fd_to_client, client_mapping, (khint_t)fd);
    assert(key != kh_end(client_mapping));
    cdb_client *client = kh_value(client_mapping, key);
    kh_del(fd_to_client, client_mapping, key);

    cdb_client_destroy(client);
}


void cdb_client_sendcounter(cdb_client *client, counter_t counter)
{
    sds msg = sdsempty();
    msg = sdscatprintf(msg, "%zu\n", counter);
    cdb_client_add_reply(client, msg);
}

void cdb_client_sendstrcnt(cdb_client *client, char *str, counter_t counter)
{
    sds msg = sdsempty();
    msg = sdscatprintf(msg, "%s %lu\n", str, counter);
    cdb_client_add_reply(client, msg);
}

void cdb_client_sendsize(cdb_client *client, size_t size)
{
    sds msg = sdsempty();
    msg = sdscatprintf(msg, "%zu\n", size);
    cdb_client_add_reply(client, msg);
}

void cdb_client_sendstr(cdb_client *client, char *str)
{
    sds msg = sdsempty();
    msg = sdscatprintf(msg, "%s\n", str);
    cdb_client_add_reply(client, msg);
}

void cdb_client_sendstrstrset(cdb_client *client, htable_t *map)
{
    cdb_client_sendsize(client, htable_size(map));

    htable_for_each(item, map) {
        char *key = htable_key(item);
        cdb_client_sendstr(client, key);

        htable_t *set = htable_value(item);
        cdb_client_sendsize(client, htable_size(set));

        htable_for_each(set_item, set) {
            char *set_key = htable_key(set_item);
            cdb_client_sendstr(client, set_key);
        }
    }
}

void cdb_client_sendstrlist(cdb_client *client, char **list, size_t list_size)
{
    cdb_client_sendsize(client, list_size);
    for (size_t i = 0; i < list_size; i++ )
        cdb_client_sendstr(client, list[i]);
}

void cdb_client_sendstrcntmap(cdb_client *client, htable_t *map)
{
    cdb_client_sendsize(client, htable_size(map));

    htable_for_each(item, map) {
        char *key = htable_key(item);
        counter_t *count = htable_value(item);
        cdb_client_sendstrcnt(client, key, *count);
    }
}

void cdb_client_sendstrstrcntmap(cdb_client *client, htable_t *map)
{
    cdb_client_sendsize(client, htable_size(map));

    htable_for_each(item, map) {
        char *key = htable_key(item);
        cdb_client_sendstr(client, key);

        htable_t *inner_map = htable_value(item);
        cdb_client_sendstrcntmap(client, inner_map);
    }
}

void cdb_client_sendcode(cdb_client *client, cmd_reply code) {
    sds msg = sdsempty();
    msg = sdscatprintf(msg, "%d\n", code);
    cdb_client_add_reply(client, msg);
}
