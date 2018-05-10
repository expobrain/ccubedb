#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
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
#include "htable.h"
#include "slist.h"
#include "cdb_defs.h"
#include "cdb_cubedb.h"
#include "cdb_cube.h"
#include "cdb_network.h"
#include "cdb_client.h"
#include "cdb_config.h"
#include "cdb_alloc.h"
#include "cdb_log.h"
#include "cdb_dump.h"

/* The database itself, to be init-ed in main */
static cdb_cubedb *cubedb;

/* Server configuration itself, to be init-ed in main */
cdb_config *config;

/* A map of fds to clients */
khash_t(fd_to_client) *client_mapping = NULL;

/* Cubedb commands and the command table */

typedef enum cmd_result cmd_result;
enum cmd_result {
    CMD_DONE,                   /* Everything is fine */
    CMD_QUIT,                   /* Disconnect immediately*/
};

typedef cmd_result cmd_function(cdb_client *client, sds *argv, int argc);

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

static cmd_result cmd_quit(cdb_client *client, sds *argv, int argc)
{
    (void) argv; (void) argc; (void) client;
    return CMD_QUIT;
}

static cmd_result cmd_ping(cdb_client *client, sds *argv, int argc)
{
    (void) argv; (void) argc;
    cdb_client_sendstr(client, "PONG");
    return CMD_DONE;
}

static cmd_result cmd_cubes(cdb_client *client, sds *argv, int argc)
{
    (void) argv; (void) argc;

    size_t cube_count = 0;
    char **cube_names = cdb_cubedb_get_cube_names(cubedb, &cube_count);
    defer { free(cube_names); }

    cdb_client_sendstrlist(client, cube_names, cube_count);

    return CMD_DONE;
}

static cmd_result cmd_add_cube(cdb_client *client, sds *argv, int argc)
{
    (void) argc; (void) client;

    sds cube_name = argv[1];
    if (cdb_cubedb_find_cube(cubedb, cube_name)) {
        cdb_client_sendcode(client, REPLY_ERR_OBJ_EXISTS);
        return CMD_DONE;
    }

    cdb_cube *cube = cdb_cube_create();
    cdb_cubedb_add_cube(cubedb, cube_name, cube);
    cdb_client_sendcode(client, REPLY_OK);

    return CMD_DONE;
}

static cmd_result cmd_del_cube(cdb_client *client, sds *argv, int argc)
{
    (void) argc; (void) client;

    sds cube_name = argv[1];

    cdb_cube *cube = cdb_cubedb_find_cube(cubedb, cube_name);
    if (!cube) {
        cdb_client_sendcode(client, REPLY_ERR_OBJ_NOT_FOUND);
        return CMD_DONE;
    }

    cdb_cubedb_del_cube(cubedb, cube_name);
    cdb_client_sendcode(client, REPLY_OK);

    return CMD_DONE;
}

static cmd_result cmd_part(cdb_client *client, sds *argv, int argc)
{
    sds cube_name = argv[1];

    cdb_cube *cube = cdb_cubedb_find_cube(cubedb, cube_name);
    if (!cube) {
        cdb_client_sendcode(client, REPLY_ERR_OBJ_NOT_FOUND);
        return CMD_DONE;
    }

    htable_t *column_to_value_set = NULL;
    if (4 == argc) {
        sds from_partition = parse_nullable_arg(argv[2]);
        sds to_partition = parse_nullable_arg(argv[3]);
        column_to_value_set = cdb_cube_get_columns_to_value_set(cube, from_partition, to_partition);
    } else if (3 == argc) {
        sds partition = parse_nullable_arg(argv[2]);
        if (!partition) {
            cdb_client_sendcode(client, REPLY_ERR_WRONG_ARG);
            return CMD_DONE;
        }
        if (!cdb_cube_has_partition(cube, partition)) {
            cdb_client_sendcode(client, REPLY_ERR_OBJ_NOT_FOUND);
            return CMD_DONE;
        }
        column_to_value_set = cdb_cube_get_columns_to_value_set(cube, partition, partition);
    } else {
        column_to_value_set = cdb_cube_get_columns_to_value_set(cube, NULL, NULL);
    }
    defer { htable_destroy(column_to_value_set); }

    cdb_client_sendstrstrset(client, column_to_value_set);
    return CMD_DONE;
}

static cmd_result cmd_del_cube_partition(cdb_client *client, sds *argv, int argc)
{
    (void) client;

    sds cube_name = argv[1];

    cdb_cube *cube = cdb_cubedb_find_cube(cubedb, cube_name);
    if (!cube) {
        cdb_client_sendcode(client, REPLY_ERR_OBJ_NOT_FOUND);
        return CMD_DONE;
    }

    if (4 == argc) {
        sds from_partition = parse_nullable_arg(argv[2]);
        sds to_partition = parse_nullable_arg(argv[3]);
        cdb_cube_delete_partition_from_to(cube, from_partition, to_partition);
    } else if (3 == argc) {
        sds partition_name = argv[2];
        if (!cdb_cube_has_partition(cube, partition_name)) {
            cdb_client_sendcode(client, REPLY_ERR_OBJ_NOT_FOUND);
            return CMD_DONE;
        }
        cdb_cube_delete_partition_from_to(cube, partition_name, partition_name);
    } else {
        assert(false);
    }

    cdb_client_sendcode(client, REPLY_OK);
    return CMD_DONE;
}

static cmd_result cmd_cube(cdb_client *client, sds *argv, int argc)
{
    (void) argc;

    sds cube_name = argv[1];
    cdb_cube *cube = cdb_cubedb_find_cube(cubedb, cube_name);
    if (!cube) {
        cdb_client_sendcode(client, REPLY_ERR_OBJ_NOT_FOUND);
        return CMD_DONE;
    }

    size_t partition_count = 0;
    char **partition_names = cdb_cube_get_partition_names(cube, &partition_count);

    cdb_client_sendstrlist(client, partition_names, partition_count);

    return CMD_DONE;
}

static cmd_result cmd_insert(cdb_client *client, sds *argv, int argc)
{
    (void) argc;

    /* No need to parse anything in partition_name */
    sds partition_name = argv[2];

    /* Parse the counter */
    sds counter_str = argv[4];
    counter_t counter = strtoul(counter_str, NULL, 0);
    if (ULONG_MAX == counter) {
        cdb_client_sendcode(client, REPLY_ERR_WRONG_ARG);
        return CMD_DONE;
    }

    cdb_insert_row *row = cdb_insert_row_create(partition_name, counter);
    defer { cdb_insert_row_destroy(row); }

    /* Parse the value list and inititialize the row to be inserted */
    sds column_to_value_list = argv[3];
    if (!cdb_insert_row_parse_values(row, column_to_value_list)) {
        cdb_client_sendcode(client, REPLY_ERR_WRONG_ARG);
        return CMD_DONE;
    }

    sds cube_name = argv[1];
    cdb_cube *cube = cdb_cubedb_find_cube(cubedb, cube_name);
    if (!cube) {
        cube = cdb_cube_create();
        cdb_cubedb_add_cube(cubedb, cube_name, cube);
    }

    if (!cdb_cube_insert_row(cube, row)) {
        cdb_client_sendcode(client, REPLY_ERR_ACTION_FAILED);
        return CMD_DONE;
    }

    cdb_client_sendcode(client, REPLY_OK);
    return CMD_DONE;
}

static cmd_result cmd_count(cdb_client *client, sds *argv, int argc)
{
    sds cube_name = argv[1];
    cdb_cube *cube = cdb_cubedb_find_cube(cubedb, cube_name);
    if (!cube) {
        cdb_client_sendcode(client, REPLY_ERR_OBJ_NOT_FOUND);
        return CMD_DONE;
    }

    sds from_partition = NULL;
    if (argc >= 3)
        from_partition = parse_nullable_arg(argv[2]);

    sds to_partition = NULL;
    if (argc >= 4)
        to_partition = parse_nullable_arg(argv[3]);

    cdb_filter *filter = NULL;
    defer { if (filter) cdb_filter_destroy(filter); }
    if (argc >= 5) {
        int res = 0;
        filter = cdb_filter_parse_from_args(argv[4], &res);
        if (res != 0) {
            cdb_client_sendcode(client, REPLY_ERR_WRONG_ARG);
            return CMD_DONE;
        }
    }

    sds group_column = NULL;
    if (6 == argc)
        group_column = parse_nullable_arg(argv[5]);

    void *result = cdb_cube_count_from_to(cube, from_partition, to_partition, filter, group_column);

    if (!group_column) {
        counter_t *count = result;
        defer { free(count); }
        cdb_client_sendcounter(client, *count);
    } else {
        htable_t *value_to_count = result;
        defer { htable_destroy(value_to_count); }

        cdb_client_sendstrcntmap(client, value_to_count);
    }

    return CMD_DONE;
}

static cmd_result cmd_pcount(cdb_client *client, sds *argv, int argc)
{
    sds cube_name = argv[1];
    cdb_cube *cube = cdb_cubedb_find_cube(cubedb, cube_name);
    if (!cube) {
        cdb_client_sendcode(client, REPLY_ERR_OBJ_NOT_FOUND);
        return CMD_DONE;
    }

    sds from_partition = NULL;
    if (argc >= 3)
        from_partition = parse_nullable_arg(argv[2]);

    sds to_partition = NULL;
    if (argc >= 4)
        to_partition = parse_nullable_arg(argv[3]);

    cdb_filter *filter = NULL;
    defer { if (filter) cdb_filter_destroy(filter); }
    if (argc >= 5) {
        int res = 0;
        filter = cdb_filter_parse_from_args(argv[4], &res);
        if (res != 0) {
            cdb_client_sendcode(client, REPLY_ERR_WRONG_ARG);
            return CMD_DONE;
        }
    }

    sds group_column = NULL;
    if (6 == argc)
        group_column = parse_nullable_arg(argv[5]);

    htable_t *partition_to_result = cdb_cube_pcount_from_to(cube, from_partition, to_partition, filter, group_column);
    defer { htable_destroy(partition_to_result); }

    if (!group_column) {
        /* Just a per-partition counter if there was not group_column specified */
        cdb_client_sendstrcntmap(client, partition_to_result);
    } else {
        /* For grouped results dump an htable of values to counters for every partition */
        cdb_client_sendstrstrcntmap(client, partition_to_result);
    }

    return CMD_DONE;
}

static cmd_result cmd_dump(cdb_client *client, sds *argv, int argc)
{
    (void) argv; (void) argc;

    if (!config->dump_path) {
        log_warn("Dump path not supplied upon start, aborting");
        cdb_client_sendcode(client, REPLY_ERR_CONFIGURATION_ERR);
        return CMD_DONE;
    }

    if (cdb_do_dump(config->dump_path, cubedb))
        cdb_client_sendcode(client, REPLY_ERR_ACTION_FAILED);
    else
        cdb_client_sendcode(client, REPLY_OK);;

    return CMD_DONE;
}

static cmd_result cmd_help(cdb_client *client, sds *argv, int argc)
{
    (void) argc; (void) argv;

    size_t table_size = 0;
    while (NULL != cmd_table[table_size].name)
        table_size++;

    cdb_client_sendsize(client, table_size);

    for (size_t i = 0; i < table_size; i++)
        cdb_client_sendstr(client, cmd_table[i].description);

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

    { "DUMP", cmd_dump, 0, 0,
      "DUMP: dump cube data into a preconfigured path"},

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

static bool is_correct_cmd_arg(const char *arg)
{
    while (*arg && isprint(*arg))
        arg++;
    return '\0' == *arg;
}

static cmd_result process_cmd(cdb_client *client, sds query)
{
    /* Check size */
    if (sdslen(query) > MAX_QUERY_SIZE) {
        cdb_client_sendcode(client, REPLY_ERR_QUERY_TOO_LONG);
        return CMD_DONE;
    }

    /* Split the query into cmd + arguments */
    int argc = 0;
    sds *argv = sdssplitargs(query ,&argc);
    if (!argv){
        cdb_client_sendcode(client, REPLY_ERR_WRONG_ARG);
        return CMD_DONE;
    }

    defer { sdsfreesplitres(argv, argc); }

    sds command = sdsdup(argv[0]);
    defer { sdsfree(command); }
    sdstoupper(command);

    for (int i = 0; i < argc; i++)
        if (!is_correct_cmd_arg(argv[i])) {
            cdb_client_sendcode(client, REPLY_ERR_MALFORMED_ARG);
            return CMD_DONE;
        }

    cubedb_cmd *cmd = find_cmd(command);
    if (!cmd) {
        cdb_client_sendcode(client, REPLY_ERR_NOT_FOUND);
        return CMD_DONE;
    }

    if (cmd->min_arity > argc - 1 || cmd->max_arity < argc - 1) {
        cdb_client_sendcode(client, REPLY_ERR_WRONG_ARG_NUM);
        return CMD_DONE;
    }

    return cmd->cmd(client, argv, argc);
}

/* TCP server utilities */

static int read_from_client(cdb_client *client)
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

static void do_server_loop(const char *port, int connection_num)
{
    /* The server, using select only for now */
    int listener_fd;
    {
        /* Get a list of bindable network addresses */
        struct addrinfo *servinfo = cdb_find_bindable_addr(port);
        defer { freeaddrinfo(servinfo); }

        /* Bind the first bindable address */
        listener_fd = cdb_bind_addr(servinfo);

        /* Accept things, finally */
        if (-1 == listen(listener_fd, connection_num)) {
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

            /* Otherwise something went seriously wrong, abort */
            perror("select");
            exit(EXIT_FAILURE);
        }

        for (int this_fd = 0; this_fd <= fdmax; this_fd++) {

            /* Handle read events */

            if (FD_ISSET(this_fd, &read_fds)) {

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

                    cdb_client *client = cdb_client_create(new_fd);
                    cdb_client_register(client, &their_addr);

                    log_info("Connection accepted from %s", client->addrstr);
                    continue;
                }

                /* New data from the client */

                cdb_client *client = cdb_client_find(this_fd);
                assert(client);
                int res = read_from_client(client);
                if (res <= 0) {
                    if (0 == res)
                        log_info("Connection closed with %s", client->addrstr);
                    else if (-1 == res)
                        log_warn("Connection error with %s", client->addrstr);
                    FD_CLR(client->fd, &master);
                    FD_CLR(client->fd, &write_fds);
                    cdb_client_unregister(client->fd);
                } else {
                    /* Otherwise just go on with reading more data */
                }
            }

            /* Handle write events */

            if (FD_ISSET(this_fd, &write_fds)) {
                cdb_client *client = cdb_client_find(this_fd);
                assert(client);
                if (!cdb_client_has_replies(client))
                    continue;

                int replies_sent = cdb_client_send_replies(client);
                if (replies_sent < 0) {
                    log_warn("Connection error with %s", client->addrstr);
                    FD_CLR(client->fd, &master);
                    cdb_client_unregister(client->fd);
                }
            }
        }
    }
}

int main(int argc, char **argv)
{
    /* Global state init */
    cubedb = cdb_cubedb_create();
    config = cdb_config_create(argc, argv);
    client_mapping = cdb_client_mapping_init();

    /* Check if a dump is available */
    if (config->dump_path) {
        log_info("Loading dumped cubes from %s", config->dump_path);
        int files_loaded = cdb_load_dump(config->dump_path, cubedb);
        log_info("%d dumps loaded", files_loaded);
    }

    /* Launch the server */
    do_server_loop(config->port, config->connection_num);

    return EXIT_SUCCESS;
}
