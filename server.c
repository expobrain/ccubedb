#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/wait.h>
#include <signal.h>
#include <assert.h>
#include <limits.h>
#include <time.h>

#include "sds.h"
#include "defs.h"
#include "cubedb.h"
#include "cube.h"
#include "network.h"
#include "client.h"
#include "htable.h"
#include "slist.h"
#include "config.h"
#include "cdb_alloc.h"
#include "log.h"

/* The database itself, to be init-ed in main */
static cubedb_t *cubedb;

/* Server configuration itself, to be init-ed in main */
config_t *config;

/* Cubedb commands and the command table */

typedef enum cmd_result cmd_result;
enum cmd_result {
    CMD_DONE,                   /* Everything is fine */
    CMD_QUIT,                   /* Disconnect immediately*/
};

typedef cmd_result cmd_function(client_t *client, sds *argv, int argc);

typedef struct cubedb_cmd cubedb_cmd;
struct cubedb_cmd {
    char *name;
    cmd_function *cmd;
    int min_arity;
    int max_arity;
    char *description;
};

static cubedb_cmd cmd_table[];

static inline sds parse_nullable_arg(sds arg)
{
    if (0 == sdslen(arg) || 0 == strcmp("null", arg))
        return NULL;
    else
        return arg;
}

static cmd_result cmd_quit(client_t *client, sds *argv, int argc)
{
    (void) argv; (void) argc; (void) client;
    return CMD_QUIT;
}

static cmd_result cmd_ping(client_t *client, sds *argv, int argc)
{
    (void) argv; (void) argc;
    client_sendstr(client, "PONG");
    return CMD_DONE;
}

static cmd_result cmd_cubes(client_t *client, sds *argv, int argc)
{
    (void) argv; (void) argc;

    size_t cube_count = 0;
    char **cube_names = cubedb_get_cube_names(cubedb, &cube_count);
    defer { free(cube_names); }

    client_sendstrlist(client, cube_names, cube_count);

    return CMD_DONE;
}

static cmd_result cmd_add_cube(client_t *client, sds *argv, int argc)
{
    (void) argc; (void) client;

    sds cube_name = argv[1];
    if (cubedb_find_cube(cubedb, cube_name)) {
        client_sendcode(client, REPLY_ERR_OBJ_EXISTS);
        return CMD_DONE;
    }

    cube_t *cube = cube_create();
    cubedb_add_cube(cubedb, cube_name, cube);
    client_sendcode(client, REPLY_OK);

    return CMD_DONE;
}

static cmd_result cmd_del_cube(client_t *client, sds *argv, int argc)
{
    (void) argc; (void) client;

    sds cube_name = argv[1];

    cube_t *cube = cubedb_find_cube(cubedb, cube_name);
    if (!cube) {
        client_sendcode(client, REPLY_ERR_OBJ_NOT_FOUND);
        return CMD_DONE;
    }

    cubedb_del_cube(cubedb, cube_name);
    client_sendcode(client, REPLY_OK);

    return CMD_DONE;
}

static cmd_result cmd_part(client_t *client, sds *argv, int argc)
{
    sds cube_name = argv[1];

    cube_t *cube = cubedb_find_cube(cubedb, cube_name);
    if (!cube) {
        client_sendcode(client, REPLY_ERR_OBJ_NOT_FOUND);
        return CMD_DONE;
    }

    htable_t *column_to_value_set = NULL;
    if (4 == argc) {
        sds from_partition = parse_nullable_arg(argv[2]);
        sds to_partition = parse_nullable_arg(argv[3]);
        column_to_value_set = cube_get_columns_to_value_set(cube, from_partition, to_partition);
    } else if (3 == argc) {
        sds partition = parse_nullable_arg(argv[2]);
        if (!partition) {
            client_sendcode(client, REPLY_ERR_WRONG_ARG);
            return CMD_DONE;
        }
        if (!cube_has_partition(cube, partition)) {
            client_sendcode(client, REPLY_ERR_OBJ_NOT_FOUND);
            return CMD_DONE;
        }
        column_to_value_set = cube_get_columns_to_value_set(cube, partition, partition);
    } else {
        column_to_value_set = cube_get_columns_to_value_set(cube, NULL, NULL);
    }
    defer { htable_destroy(column_to_value_set); }

    client_sendstrstrset(client, column_to_value_set);
    return CMD_DONE;
}

static cmd_result cmd_del_cube_partition(client_t *client, sds *argv, int argc)
{
    (void) client;

    sds cube_name = argv[1];

    cube_t *cube = cubedb_find_cube(cubedb, cube_name);
    if (!cube) {
        client_sendcode(client, REPLY_ERR_OBJ_NOT_FOUND);
        return CMD_DONE;
    }

    if (4 == argc) {
        sds from_partition = parse_nullable_arg(argv[2]);
        sds to_partition = parse_nullable_arg(argv[3]);
        cube_delete_partition_from_to(cube, from_partition, to_partition);
    } else if (3 == argc) {
        sds partition_name = argv[2];
        if (!cube_has_partition(cube, partition_name)) {
            client_sendcode(client, REPLY_ERR_OBJ_NOT_FOUND);
            return CMD_DONE;
        }
        cube_delete_partition_from_to(cube, partition_name, partition_name);
    } else {
        assert(false);
    }

    client_sendcode(client, REPLY_OK);
    return CMD_DONE;
}

static cmd_result cmd_cube(client_t *client, sds *argv, int argc)
{
    (void) argc;

    sds cube_name = argv[1];
    cube_t *cube = cubedb_find_cube(cubedb, cube_name);
    if (!cube) {
        client_sendcode(client, REPLY_ERR_OBJ_NOT_FOUND);
        return CMD_DONE;
    }

    size_t partition_count = 0;
    char **partition_names = cube_get_partition_names(cube, &partition_count);

    client_sendstrlist(client, partition_names, partition_count);

    return CMD_DONE;
}

static cmd_result cmd_insert(client_t *client, sds *argv, int argc)
{
    (void) argc; (void) client;

    sds cube_name = argv[1];
    cube_t *cube = cubedb_find_cube(cubedb, cube_name);
    if (!cube) {
        client_sendcode(client, REPLY_ERR_OBJ_NOT_FOUND);
        return CMD_DONE;
    }

    /* No need to parse anything in partition_name */
    sds partition_name = argv[2];

    /* Parse the counter */
    sds counter_str = argv[4];
    counter_t counter = strtoul(counter_str, NULL, 0);
    if (ULONG_MAX == counter) {
        client_sendcode(client, REPLY_ERR_WRONG_ARG);
        return CMD_DONE;
    }

    insert_row_t *row = insert_row_create(partition_name, counter);
    defer { insert_row_destroy(row); }

    /* Parse the value list and inititialize the row to be inserted */
    sds column_to_value_list = argv[3];
    int cv_pair_num = 0;
    sds *cv_pair = sdssplitlen(column_to_value_list,
                               sdslen(column_to_value_list),
                               "&", 1,
                               &cv_pair_num);
    defer { sdsfreesplitres(cv_pair, cv_pair_num); }

    for (size_t i = 0; i < (size_t)cv_pair_num; i++ ) {
        sds pair = cv_pair[i];
        if (!pair) continue;

        int pair_tokens_len = 0;
        sds *pair_tokens = sdssplitlen(pair, sdslen(pair), "=", 1, &pair_tokens_len);
        defer { sdsfreesplitres(pair_tokens, pair_tokens_len); }

        if (2 != pair_tokens_len) {
            client_sendcode(client, REPLY_ERR_WRONG_ARG);
            return CMD_DONE;
        }

        sds column = pair_tokens[0];
        sds value = pair_tokens[1];

        if (insert_row_has_column(row, column)){
            client_sendcode(client, REPLY_ERR_WRONG_ARG);
            return CMD_DONE;
        }

        insert_row_add_column_value(row, column, value);
    }

    if (!cube_insert_row(cube, row)) {
        client_sendcode(client, REPLY_ERR_ACTION_FAILED);
        return CMD_DONE;
    }

    client_sendcode(client, REPLY_OK);
    return CMD_DONE;
}

static cmd_result cmd_count(client_t *client, sds *argv, int argc)
{
    sds cube_name = argv[1];
    cube_t *cube = cubedb_find_cube(cubedb, cube_name);
    if (!cube) {
        client_sendcode(client, REPLY_ERR_OBJ_NOT_FOUND);
        return CMD_DONE;
    }

    sds from_partition = NULL;
    if (argc >= 3)
        from_partition = parse_nullable_arg(argv[2]);

    sds to_partition = NULL;
    if (argc >= 4)
        to_partition = parse_nullable_arg(argv[3]);

    filter_t *filter = NULL;
    defer { if (filter) filter_destroy(filter); }
    if (argc >= 5) {
        int res = 0;
        filter = filter_parse_from_args(argv[4], &res);
        if (res != 0) {
            client_sendcode(client, REPLY_ERR_WRONG_ARG);
            return CMD_DONE;
        }
    }

    sds group_column = NULL;
    if (6 == argc)
        group_column = parse_nullable_arg(argv[5]);

    void *result = cube_count_from_to(cube, from_partition, to_partition, filter, group_column);

    if (!group_column) {
        counter_t *count = result;
        defer { free(count); }
        client_sendcounter(client, *count);
    } else {
        htable_t *value_to_count = result;
        defer { htable_destroy(value_to_count); }

        client_sendstrcntmap(client, value_to_count);
    }

    return CMD_DONE;
}

static cmd_result cmd_pcount(client_t *client, sds *argv, int argc)
{
    sds cube_name = argv[1];
    cube_t *cube = cubedb_find_cube(cubedb, cube_name);
    if (!cube) {
        client_sendcode(client, REPLY_ERR_OBJ_NOT_FOUND);
        return CMD_DONE;
    }

    sds from_partition = NULL;
    if (argc >= 3)
        from_partition = parse_nullable_arg(argv[2]);

    sds to_partition = NULL;
    if (argc >= 4)
        to_partition = parse_nullable_arg(argv[3]);

    filter_t *filter = NULL;
    defer { if (filter) filter_destroy(filter); }
    if (argc >= 5) {
        int res = 0;
        filter = filter_parse_from_args(argv[4], &res);
        if (res != 0) {
            client_sendcode(client, REPLY_ERR_WRONG_ARG);
            return CMD_DONE;
        }
    }

    sds group_column = NULL;
    if (6 == argc)
        group_column = parse_nullable_arg(argv[5]);

    htable_t *partition_to_result = cube_pcount_from_to(cube, from_partition, to_partition, filter, group_column);
    defer { htable_destroy(partition_to_result); }

    if (!group_column) {
        /* Just a per-partition counter if there was not group_column specified */
        client_sendstrcntmap(client, partition_to_result);
    } else {
        /* For grouped results dump an htable of values to counters for every partition */
        client_sendstrstrcntmap(client, partition_to_result);
    }

    return CMD_DONE;
}

static cmd_result cmd_help(client_t *client, sds *argv, int argc)
{
    (void) argc; (void) argv;

    size_t table_size = 0;
    while (NULL != cmd_table[table_size].name)
        table_size++;

    client_sendsize(client, table_size);

    for (size_t i = 0; i < table_size; i++)
        client_sendstr(client, cmd_table[i].description);

    return CMD_DONE;
}

static cubedb_cmd cmd_table[] = {
    { "QUIT", cmd_quit, 0, 0,
      "QUIT: disconnect"},

    { "PING", cmd_ping, 0, 0,
      "PING: reply with \"PONG\""},

    { "CUBES", cmd_cubes, 0, 0,
      "CUBES: list all the cubes"},

    { "ADDCUBE", cmd_add_cube, 1, 1,
      "ADDCUBE <name>: add a cube with a given <name>"},

    { "CUBE", cmd_cube, 1, 1,
      "CUBE <name>: list cube <name> partitions"},

    { "DELCUBE", cmd_del_cube, 1, 1,
      "DELCUBE <name>: delete a cube with a given <name>"},

    { "PART", cmd_part, 1, 3,
      "PART <cube> [(<partition> | <from> <to>)]"
      "list <cube> partition (or a <from>/<to> partition range) columns and column values"},

    { "DELPART", cmd_del_cube_partition, 2, 3,
      "DELPART <cube> (<partition> | <from> <to>): "
      "delete a cube <cube> partition <partition> or partitions between <from>/<to>"},

    { "INSERT", cmd_insert, 4, 4,
      "INSERT <cube> <partition> <columnvalues> <count>: "
      "insert a row into a <cube> partition <partition> with given <columnvalues>, count <count>"},

    { "COUNT", cmd_count, 1, 5,
      "COUNT <cube> [<from> [<to> [<columnvalues> [<groupcolumn>]]]]: "
      "count the sum of row counts in a <cube> between <from>/<to> partitions, inclusive, "
      "with optional given <columnvalues> (can be \"null\") <groupcolumn>"},

    { "PCOUNT", cmd_pcount, 1, 5,
      "PCOUNT <cube> [<from> [<to> [<columnvalues> [<groupcolumn>]]]]: "
      "count the per-partition sum of row counts in a <cube> between <from>/<to> partitions, inclusive, "
      "with optional <columnvalues> (can be \"null\") and <groupcolumn>"},

    { "HELP", cmd_help, 0, 0,
      "HELP: dump descriptions of all commands available"},

    { NULL }
};

static cubedb_cmd *find_cmd(char *name)
{
    size_t i = 0;
    while (NULL != cmd_table[i].name) {
        if (0 == strcmp(cmd_table[i].name, name))
            return &cmd_table[i];
        i++;
    }
    return NULL;
}

bool is_correct_cmd_arg(const char *arg)
{
    while (*arg && isprint(*arg))
        arg++;
    return '\0' == *arg;
}

cmd_result process_cmd(client_t *client, sds query)
{
    /* Split the query into cmd + arguments */
    int argc = 0;
    sds *argv = sdssplitargs(query ,&argc);
    if (!argv){
        client_sendcode(client, REPLY_ERR_WRONG_ARG);
        return CMD_DONE;
    }

    defer { sdsfreesplitres(argv, argc); }

    sds command = sdsdup(argv[0]);
    defer { sdsfree(command); }
    sdstoupper(command);

    for (int i = 0; i < argc; i++)
        if (!is_correct_cmd_arg(argv[i])) {
            client_sendcode(client, REPLY_ERR_MALFORMED_ARG);
            return CMD_DONE;
        }

    cubedb_cmd *cmd = find_cmd(command);
    if (!cmd) {
        client_sendcode(client, REPLY_ERR_NOT_FOUND);
        return CMD_DONE;
    }

    if (cmd->min_arity > argc - 1 || cmd->max_arity < argc - 1) {
        client_sendcode(client, REPLY_ERR_WRONG_ARG_NUM);
        return CMD_DONE;
    }

    return cmd->cmd(client, argv, argc);
}

/* TCP server */

int read_from_client(client_t *client)
{
    char receive_buffer[RECEIVE_BUFSIZE] = {0};
    int receive_size = 0;

    receive_size = recv(client->fd, receive_buffer, RECEIVE_BUFSIZE, 0);
    if (-1 == receive_size) {
        perror("recv");
        return receive_size;
    } else if (0 == receive_size) {
        log_info("Client closed connection");
        return receive_size;
    }
    client->querybuf = sdscatlen(client->querybuf, receive_buffer, (size_t)receive_size);

    char *p = strchr(client->querybuf, '\n');
    if (!p) return receive_size;

    /* Now, parse and try to process query */
    {
        sds query = client->querybuf;
        client->querybuf = sdsempty();
        defer { sdsfree(query); }

        size_t querylen;
        querylen = 1 + (p - query);
        if (sdslen(query) > querylen) {
            /* Only need the data up to the newline, leave the rest in the buffer. */
            client->querybuf = sdscatlen(client->querybuf,query + querylen, sdslen(query) - querylen);
        }

        /* Strip newlines and carriage returns, if any */
        *p = '\0';
        if (*(p-1) == '\r') *(p-1) = '\0';
        sdsupdatelen(query);

        /* Ignore empty query */
        if (sdslen(query) == 0)
            return receive_size;

        /* Process the cmds received */
        clock_t start = clock();

        cmd_result result = process_cmd(client, query);

        double elapsed_time = (clock() - start)/(double)CLOCKS_PER_SEC;
        log_verb("'%s' served in %.3f seconds", query, elapsed_time);

        /* Wanna quit? Go on */
        if (CMD_QUIT == result)
            return 0;
    }

    return receive_size;
}

int main(int argc, char **argv)
{
    /* Global state init */
    cubedb = cubedb_create();
    config = config_create(argc, argv);
    client_mapping_init();

    int listener_fd;
    {
        /* Get a list of bindable network addresses */
        struct addrinfo *servinfo = find_bindable_addr(config);
        defer { freeaddrinfo(servinfo); }

        /* Bind the first bindable address */
        listener_fd = bind_addr(servinfo);

        /* Accept things, finally */
        if (-1 == listen(listener_fd, config->connections)) {
            perror("listen");
            exit(EXIT_FAILURE);
        }
    }


    /* Loop through incoming connections and handle them synchronously */
    log_info("Waiting for connections...");

    fd_set master;
    fd_set read_fds;
    fd_set write_fds;
    int fdmax = listener_fd;

    FD_ZERO(&master);
    FD_ZERO(&read_fds);
    FD_ZERO(&write_fds);
    FD_SET(listener_fd, &master);

    while(1) {
        read_fds = master;
        write_fds = master;
        if (-1 == select(fdmax + 1, &read_fds, &write_fds, NULL, NULL)) {
            /* Interrupted? Just restart the polling */
            if (EINTR == errno)
                continue;
            perror("select");
            exit(EXIT_FAILURE);
        }

        /* Handle read events */

        for (int this_fd = 0; this_fd <= fdmax; this_fd++) {
            if (!FD_ISSET(this_fd, &read_fds))
                continue;

            if (this_fd == listener_fd) {
                /* New incoming connection */

                struct sockaddr_storage their_addr;
                socklen_t sin_size = sizeof(their_addr);
                int new_fd = accept(listener_fd, (struct sockaddr *)&their_addr, &sin_size);
                if (new_fd == -1) {
                    perror("accept");
                    continue;
                }

                FD_SET(new_fd, &master);
                if (new_fd > fdmax)
                    fdmax = new_fd;

                client_t *client = client_create(new_fd);
                client_register(client, &their_addr);

                log_info("Connection accepted from %s", client->addrstr);
            } else {
                /* New data from the client */

                client_t *client = client_find(this_fd);
                assert(client);
                int res = read_from_client(client);
                if (res <= 0) {
                    if (0 == res)
                        log_info("Connection closed with %s", client->addrstr);
                    else if (-1 == res)
                        log_warn("Connection error with %s", client->addrstr);
                    FD_CLR(client->fd, &master);
                    FD_CLR(client->fd, &write_fds);
                    client_unregister(client->fd);
                } else {
                    /* Otherwise just go on with reading more data */
                }
            }
        }

        /* Handle write events */

        for (int this_fd = 0; this_fd <= fdmax; this_fd++) {
            if (!FD_ISSET(this_fd, &write_fds))
                continue;
            if (this_fd == listener_fd)
                continue;

            client_t *client = client_find(this_fd);
            assert(client);

            if (!client_has_replies(client))
                continue;

            int replies_sent = client_send_replies(client);
            if (replies_sent < 0) {
                log_warn("Connection error with %s", client->addrstr);
                FD_CLR(client->fd, &master);
                client_unregister(client->fd);
            } else {
                /* Either we had nothing so send or couldn't send all of it*/
            }
        }
    }
    return EXIT_SUCCESS;
}
