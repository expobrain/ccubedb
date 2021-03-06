#define _GNU_SOURCE
#include <ftw.h>

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

#include "cdb_dump.h"
#include "cdb_defs.h"
#include "cdb_alloc.h"
#include "cdb_log.h"
#include "slist.h"
#include "sds.h"

static bool ends_with(const char *string, const char *suffix)
{
    if( ! string || ! suffix )
        return false;
    size_t string_len = strlen(string);
    size_t suffix_len = strlen(suffix);
    return strcmp(string + string_len - suffix_len, suffix) == 0;
}

static int parse_and_insert(const char *query, cdb_cubedb *cdb)
{
    /* TODO: there's a lot of duplication between cmd_insert and parse_and_insert */

    int argc = 0;
    sds *argv = sdssplitargs(query ,&argc);
    if (!argv)
        return -1;
    defer { sdsfreesplitres(argv, argc); }

    /* Insert arity is always the same */
    if (argc != 5)
        return -1;

    sds command = sdsdup(argv[0]);
    defer { sdsfree(command); }

    sdstoupper(command);

    /* Only inserts are possible in dumps */
    if (strcmp(command, "INSERT") != 0)
        return -1;

    /* Prepare a row for insertion */
    sds partition_name = argv[2];

    sds counter_str = argv[4];
    counter_t counter = strtoul(counter_str, NULL, 0);
    if (ULONG_MAX == counter)
        return -1;

    cdb_insert_row *row = cdb_insert_row_create(partition_name, counter);
    defer { cdb_insert_row_destroy(row); }

    sds column_to_value_list = argv[3];
    if (!cdb_insert_row_parse_values(row, column_to_value_list))
        return -1;

    /* Get or create the cube in question */
    sds cube_name = argv[1];

    cdb_cube *cube = cdb_cubedb_find_cube(cdb, cube_name);
    if (!cube) {
        cube = cdb_cube_create();
        cdb_cubedb_add_cube(cdb, cube_name, cube);
    }

    /* And insert the row */
    if (!cdb_cube_insert_row(cube, row))
        return -1;

    return 0;
}

static int load_cube_file(const char *dump_file, cdb_cubedb *cdb)
{
    char query_buf[MAX_QUERY_SIZE];
    FILE * fp = fopen(dump_file, "r");
    if (!fp) {
        perror("fopen");
        return -1;
    }
    defer { fclose(fp); }

    while(fgets(query_buf, MAX_QUERY_SIZE, fp) != NULL) {
        if (parse_and_insert(query_buf, cdb))
            log_warn("Failed to execute \"%s\" ", query_buf);
    }

    return 0;
}

int cdb_load_dump(const char *dump_dir, cdb_cubedb *cdb)
{
    slist_t *cube_file_list = slist_create();
    defer {
        slist_for_each(node, cube_file_list) {
            char *file_path = slist_data(node);
            free(file_path);
        }
        slist_destroy(cube_file_list);
    }

    int find_cubes(const char *fpath, const struct stat *sb,
                   int typeflag, struct FTW *ftwbuf) {
        (void)sb; (void) ftwbuf;

        if (typeflag != FTW_F)
            return 0;

        if (!ends_with(fpath, ".cdb"))
            return 0;

        slist_append(cube_file_list, strdup(fpath));
        return 0;
    }

    if (0 != nftw(dump_dir, find_cubes, 200, 0)) {
        perror("nftw");
        exit(EXIT_FAILURE);
    }

    slist_for_each(node, cube_file_list) {
        char *dump_file_path = slist_data(node);
        if (load_cube_file(dump_file_path, cdb)) {
            log_warn("Failed to load a dump: %s", dump_file_path);
            exit(EXIT_FAILURE);
        }
    }

    return slist_size(cube_file_list);
}

typedef struct cdb_dump_state {
    sds cube_name;
    sds partition_name;
    FILE *target_file;
    int res;
} cdb_dump_state;

static void row_visitor(cdb_insert_row *row, void *state)
{
    cdb_dump_state * dump_state = state;

    sds row_str = sdsempty();
    defer {sdsfree(row_str); }

    size_t column_value_pair_num = htable_size(row->column_to_value);
    sds *column_value_pairs = calloc(column_value_pair_num, sizeof(sds));
    defer {
        for (size_t i = 0; i < column_value_pair_num; i++ )
            sdsfree(column_value_pairs[i]);
        free(column_value_pairs);
    }

    size_t pair_index = 0;
    htable_for_each(item, row->column_to_value) {
        char *column = htable_key(item);
        char *value = htable_value(item);

        column_value_pairs[pair_index] = sdscatprintf(sdsempty(), "%s=%s", column, value);
        pair_index++;
    }
    sds joined_pairs = sdsjoinsds(column_value_pairs, column_value_pair_num, "&", 1);
    defer { sdsfree(joined_pairs); }

    row_str = sdscatprintf(
        row_str, "INSERT %s %s %s %zu\n",
        dump_state->cube_name, dump_state->partition_name, joined_pairs, row->count
    );

    if (!fwrite(row_str, sdslen(row_str), 1, dump_state->target_file))
        log_warn("Dump fwrite fail occured");
}

static void partition_visitor(sds partition_name, cdb_partition * partition, void *state)
{
    cdb_dump_state * dump_state = state;

    dump_state->partition_name = partition_name;

    cdb_partition_for_each_row(partition, row_visitor, dump_state);
}

int cdb_do_dump(const char *dump_dir, cdb_cubedb *cdb)
{
    int res = 0;

    cdb_dump_state * dump_state = calloc(1, sizeof(cdb_dump_state));
    defer { free(dump_state); }

    void cube_visitor(sds cube_name, cdb_cube * cube)
    {
        sds cube_dump_path = sdsempty();
        cube_dump_path = sdscatprintf(cube_dump_path, "%s/%s.cdb", dump_dir, cube_name);
        defer { sdsfree(cube_dump_path); }

        FILE * dump_file = fopen(cube_dump_path, "w");
        if (!dump_file) {
            perror("fopen");
            res = -1;
            return;
        }
        defer { fclose(dump_file); }

        dump_state->cube_name = cube_name;
        dump_state->target_file = dump_file;

        cdb_cube_for_each_partition(cube, partition_visitor, dump_state);
    }

    cdb_cubedb_for_each_cube(cdb, cube_visitor);

    return res;
}
