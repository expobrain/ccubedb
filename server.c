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


#define RECEIVE_BUFSIZE 512

/* The database itself, to be init-ed in main */
static cubedb_t *cubedb;

/* Server configuration itself, to be init-ed in main */
config_t *config;

/* A list of currently connected clients, to be init-ed in main */
slist_t *client_list;

/* Cubedb commands and the command table */

typedef enum cmd_result cmd_result;
enum cmd_result {
    REPLY_OK,                   /* Just reply with success code */
    REPLY_OK_NO_ANSWER,         /* Success, no need to add anything to cmd */
    REPLY_QUIT,                 /* Reply with success and disconnect*/
    REPLY_ERR,                  /* Command generic error  */
    REPLY_ERR_NOT_FOUND,        /* Command not found */
    REPLY_ERR_WRONG_ARG,        /* Command argument is wrong */
    REPLY_ERR_WRONG_ARG_NUM,    /* Command argument number is wrong */
    REPLY_ERR_MALFORMED_ARG,    /* Command argument contains non-graphic symbols*/
    REPLY_ERR_OBJ_NOT_FOUND,    /* Command object not found */
    REPLY_ERR_OBJ_EXISTS,       /* Command object already exists */
};

typedef cmd_result cmd_function(int conn_fd, sds *argv, int argc);

typedef struct cubedb_cmd cubedb_cmd;
struct cubedb_cmd {
    char *name;
    cmd_function *cmd;
    int min_arity;
    int max_arity;
    char *description;
};

static cubedb_cmd cmd_table[];

static cmd_result cmd_quit(int conn_fd, sds *argv, int argc)
{
    (void) cubedb; (void) argv; (void) argc; (void) conn_fd;
    return REPLY_QUIT;
}

static cmd_result cmd_cubes(int conn_fd, sds *argv, int argc)
{
    (void) argv; (void) argc;

    size_t cube_count = 0;
    char **cube_names = cubedb_get_cube_names(cubedb, &cube_count);
    defer { free(cube_names); }

    if (-1 == sendstrlist(conn_fd, cube_names, cube_count))
        return REPLY_ERR;

    return REPLY_OK_NO_ANSWER;
}

static cmd_result cmd_add_cube(int conn_fd, sds *argv, int argc)
{
    (void) argc; (void) conn_fd;

    sds cube_name = argv[1];
    if (cubedb_find_cube(cubedb, cube_name))
        return REPLY_ERR_OBJ_EXISTS;
    cube_t *cube = cube_create();
    cubedb_add_cube(cubedb, cube_name, cube);
    return REPLY_OK;
}

static cmd_result cmd_del_cube(int conn_fd, sds *argv, int argc)
{
    (void) argc; (void) conn_fd;

    sds cube_name = argv[1];

    cube_t *cube = cubedb_find_cube(cubedb, cube_name);
    if (!cube) return REPLY_ERR_OBJ_NOT_FOUND;

    cubedb_del_cube(cubedb, cube_name);
    return REPLY_OK;
}

static cmd_result cmd_del_cube_partition(int conn_fd, sds *argv, int argc)
{
    (void) conn_fd;

    sds cube_name = argv[1];

    cube_t *cube = cubedb_find_cube(cubedb, cube_name);
    if (!cube) return REPLY_ERR_OBJ_NOT_FOUND;

    if (4 == argc) {
        sds from_partition = argv[2];
        sds to_partition = argv[3];
        cube_delete_partition_from_to(cube, from_partition, to_partition);
    } else if (3 == argc) {
        sds partition_name = argv[2];
        if (!cube_has_partition(cube, partition_name))
            return REPLY_ERR_OBJ_NOT_FOUND;
        cube_delete_partition_from_to(cube, partition_name, partition_name);
    } else {
        assert(false);
    }

    return REPLY_OK;
}

static cmd_result cmd_cube(int conn_fd, sds *argv, int argc)
{
    (void) argc;

    sds cube_name = argv[1];
    cube_t *cube = cubedb_find_cube(cubedb, cube_name);
    if (!cube) return REPLY_ERR_OBJ_NOT_FOUND;

    size_t partition_count = 0;
    char **partition_names = cube_get_partition_names(cube, &partition_count);

    if (-1 == sendstrlist(conn_fd, partition_names, partition_count))
        return REPLY_ERR;

    return REPLY_OK_NO_ANSWER;
}

static cmd_result cmd_insert(int conn_fd, sds *argv, int argc)
{
    (void) argc; (void) conn_fd;

    sds cube_name = argv[1];
    cube_t *cube = cubedb_find_cube(cubedb, cube_name);
    if (!cube) return REPLY_ERR_OBJ_NOT_FOUND;

    /* No need to parse anything in partition_name */
    sds partition_name = argv[2];

    /* Parse the counter */
    sds counter_str = argv[4];
    counter_t counter = strtoul(counter_str, NULL, 0);
    if (ULONG_MAX == counter) return REPLY_ERR_WRONG_ARG;

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

        if (2 != pair_tokens_len) return REPLY_ERR_WRONG_ARG;

        sds column = pair_tokens[0];
        sds value = pair_tokens[1];

        if (insert_row_has_column(row, column))
            return REPLY_ERR_WRONG_ARG;

        insert_row_add_column_value(row, column, value);
    }

    cube_insert_row(cube, row);

    return REPLY_OK;
}

static cmd_result cmd_count(int conn_fd, sds *argv, int argc)
{
    assert(argc >= 4 && argc <= 6);

    sds cube_name = argv[1];
    cube_t *cube = cubedb_find_cube(cubedb, cube_name);
    if (!cube) return REPLY_ERR_OBJ_NOT_FOUND;

    sds from_partition = argv[2];
    sds to_partition = argv[3];

    filter_t *filter = NULL;
    defer { if (filter) filter_destroy(filter); }
    if (argc >= 5) {
        int res = 0;
        filter = filter_parse_from_args(argv[4], &res);
        if (res != 0) return REPLY_ERR_WRONG_ARG;
    }

    sds group_column = NULL;
    if (6 == argc) group_column = argv[5];

    void *result = cube_count_from_to(cube, from_partition, to_partition, filter, group_column);

    if (!group_column) {
        counter_t *count = result;
        defer { free(count); }
        if (-1 == sendcounter(conn_fd, *count))
            return REPLY_ERR;
    } else {
        htable_t *value_to_count = result;
        defer { htable_destroy(value_to_count); }

        if (-1 == sendstrcntmap(conn_fd, value_to_count))
            return REPLY_ERR;
    }

    return REPLY_OK_NO_ANSWER;
}

static cmd_result cmd_pcount(int conn_fd, sds *argv, int argc)
{
    assert(argc >= 4 && argc <= 6);

    sds cube_name = argv[1];
    cube_t *cube = cubedb_find_cube(cubedb, cube_name);
    if (!cube) return REPLY_ERR_OBJ_NOT_FOUND;

    sds from_partition = argv[2];
    sds to_partition = argv[3];

    filter_t *filter = NULL;
    defer { if (filter) filter_destroy(filter); }
    if (argc >= 5) {
        int res = 0;
        filter = filter_parse_from_args(argv[4], &res);
        if (res != 0) return REPLY_ERR_WRONG_ARG;
    }

    sds group_column = NULL;
    if (6 == argc) group_column = argv[5];

    htable_t *partition_to_result = cube_pcount_from_to(cube, from_partition, to_partition, filter, group_column);
    defer { htable_destroy(partition_to_result); }

    if (!group_column) {
        /* Just a per-partition counter if there was not group_column specified */
        if (-1 == sendstrcntmap(conn_fd, partition_to_result))
            return REPLY_ERR;
    } else {
        /* For grouped results dump an htable of values to counters for every partition */
        if (-1 == sendstrstrcntmap(conn_fd, partition_to_result))
            return REPLY_ERR;
    }

    return REPLY_OK_NO_ANSWER;
}

static cmd_result cmd_help(int conn_fd, sds *argv, int argc)
{
    (void) argc; (void) argv;

    size_t table_size = 0;
    while (NULL != cmd_table[table_size].name)
        table_size++;

    if (-1 == sendsize(conn_fd, table_size))
        return REPLY_ERR;

    for (size_t i = 0; i < table_size; i++) {
        if (-1 == sendstr(conn_fd, cmd_table[i].description))
            return REPLY_ERR;
    }

    return REPLY_OK_NO_ANSWER;
}

static cubedb_cmd cmd_table[] = {
    { "QUIT", cmd_quit, 0, 0,
      "QUIT: disconnect"},

    { "CUBES", cmd_cubes, 0, 0,
      "CUBES: list all the cubes"},

    { "ADDCUBE", cmd_add_cube, 1, 1,
      "ADDCUBE <name>: add a cube with a given <name>"},

    { "CUBE", cmd_cube, 1, 1,
      "CUBE <name>: list cube <name> partitions"},

    { "DELCUBE", cmd_del_cube, 1, 1,
      "DELCUBE <name>: delete a cube with a given <name>"},

    { "DELPART", cmd_del_cube_partition, 2, 3,
      "DELPART <cube> (<partition> | <from> <to>): "
      "delete a cube <cube> partition <partition> or partitions between <from>/<to>"},

    { "INSERT", cmd_insert, 4, 4,
      "INSERT <cube> <partition> <columnvalues> <count>: "
      "insert a row into a <cube> partition <partition> with given <columnvalues>, count <count>"},

    { "COUNT", cmd_count, 3, 5,
      "COUNT <cube> <from> <to> [<columnvalues> [<groupcolumn>]]: "
      "count the sum of row counts in a <cube> between <from>/<to> partitions, inclusive, "
      "with optional given <columnvalues> (can be \"null\") <groupcolumn>"},

    { "PCOUNT", cmd_pcount, 3, 5,
      "PCOUNT <cube> <from> <to> [<columnvalues> [<groupcolumn>]]: "
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
    while (*arg && isgraph(*arg) && *arg != ' ')
        arg++;
    return '\0' == *arg;
}

cmd_result process_cmd(int conn_fd, sds *argv, int argc)
{
    assert(argc > 0);

    sds command = sdsdup(argv[0]);
    defer { sdsfree(command); }
    sdstoupper(command);

    for (int i = 0; i < argc; i++)
        if (!is_correct_cmd_arg(argv[i]))
            return REPLY_ERR_MALFORMED_ARG;

    cubedb_cmd *cmd = find_cmd(command);
    if (!cmd)
        return REPLY_ERR_NOT_FOUND;

    if (cmd->min_arity > argc - 1 || cmd->max_arity < argc - 1)
        return REPLY_ERR_WRONG_ARG_NUM;

    return cmd->cmd(conn_fd, argv, argc);
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
    client->querybuf = sdscatlen(client->querybuf, receive_buffer, receive_size);

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

        /* Split the query into raw arguments */
        int raw_argc;
        sds *raw_argv = sdssplitlen(query, sdslen(query)," ",1,&raw_argc);
        defer { free(raw_argv); }

        /* Strip empty args */
        int clean_argc = 0;
        sds *clean_argv = cdb_malloc(sizeof(clean_argv[0]) * raw_argc);
        for (int j = 0; j < raw_argc; j++) {
            if (sdslen(raw_argv[j])) {
                clean_argv[clean_argc] = raw_argv[j];
                clean_argc++;
            } else {
                sdsfree(raw_argv[j]);
            }
        }
        defer {
            for (int j = 0; j < clean_argc; j++) {
                sdsfree(clean_argv[j]);
            }
            free(clean_argv);
        }

        /* Process the cmds received */
        clock_t start = clock();
        cmd_result result = process_cmd(client->fd, clean_argv, clean_argc);
        double elapsed_time = (clock() - start)/(double)CLOCKS_PER_SEC;
        log_verb("%s served in %.3f seconds", clean_argv[0], elapsed_time);

        /* It's fine */
        if (REPLY_OK == result) {
            sendok(client->fd);
        }
        /* It's fine, no need to add anything */
        else if (REPLY_OK_NO_ANSWER == result) {
        }
        /* Wanna quit? Go on */
        else if (REPLY_QUIT == result) {
            sendok(client->fd);
            return 0;
        }
        /* Something went seriosly wrong when executing the command, worth disconnecting */
        else if (REPLY_ERR == result) {
            sendcode(client->fd, -result);
            return 0;
        }
        /* Other errors (everything above the REPLY_ERR enum value) are command-specific, no need to
         * disconnect */
        else if (REPLY_ERR < result) {
            sendcode(client->fd, -result);
        }
    }

    return receive_size;
}

int main(int argc, char **argv)
{
    /* Global state init */
    cubedb = cubedb_create();
    config = config_create(argc, argv);
    client_list = slist_create();

    int listener_fd;
    {
        /* Get a list of bindable network addresses */
        struct addrinfo *servinfo = find_bindable_addr(config);
        defer { freeaddrinfo(servinfo); }

        /* Bind the first bindable address */
        listener_fd = bind_addr(servinfo);

        /* /\* No need to block *\/ */
        /* socket_non_blocking(listener_fd); */

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
    int fdmax = listener_fd;

    FD_ZERO(&master);
    FD_ZERO(&read_fds);
    FD_SET(listener_fd, &master);

    while(1) {
        read_fds = master;
        if (-1 == select(fdmax + 1, &read_fds, NULL, NULL, NULL)) {
            /* Interrupted? Just restart */
            if (EINTR == errno)
                continue;
            perror("select");
            exit(EXIT_FAILURE);
        }

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

                inet_ntop(their_addr.ss_family,
                          get_in_addr((struct sockaddr *)&their_addr),
                          client->addrstr, sizeof(client->addrstr));

                slist_append(client_list, client);

                log_info("Connection accepted from %s", client->addrstr);
            } else {
                /* New data from the client */

                client_t *client = client_find(this_fd);
                assert(client);
                int res = read_from_client(client);
                if (0 >= res) {
                    close(client->fd);
                    FD_CLR(client->fd, &master);
                    client_delete(client->fd);
                    client_destroy(client);
                    if (0 == res)
                        log_info("Connection closed with %s", client->addrstr);
                    else if (-1 == res)
                        log_warn("Connection error with %s", client->addrstr);
                } else {
                    /* Otherwise just go on with reading more data */
                }
            }
        }
    }
    return EXIT_SUCCESS;
}
