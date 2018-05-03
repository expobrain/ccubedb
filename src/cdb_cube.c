#include <stddef.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>

#include "sds.h"
#include "cdb_cube.h"
#include "partition.h"
#include "slist.h"
#include "htable.h"
#include "cdb_alloc.h"

struct cdb_cube {
    htable_t *name_to_partition;
};

cdb_cube *cdb_cube_create(void)
{
    cdb_cube *cube = cdb_malloc(sizeof(cdb_cube));
    cdb_cube_init(cube);
    return cube;
}

static void partition_cleanup(void *partition) {
    partition_destroy(partition);
}

void cdb_cube_init(cdb_cube *cube)
{
    *cube = (typeof(*cube)) {
        .name_to_partition = htable_create(1024, partition_cleanup),
    };
}

void cdb_cube_destroy(cdb_cube *cube)
{
    htable_destroy(cube->name_to_partition);
    free(cube);
}

static inline partition_t *cdb_cube_get_partition(cdb_cube *cube, sds partition_name)
{
    return htable_get(cube->name_to_partition, partition_name);
}

static inline partition_t *cdb_cube_add_partition(cdb_cube *cube, sds partition_name)
{
    if (htable_has(cube->name_to_partition, partition_name))
        return NULL;
    partition_t *new_partition = partition_create(partition_name);
    htable_put(cube->name_to_partition, partition_name, new_partition);
    return new_partition;
}

bool cdb_cube_insert_row(cdb_cube *cube, insert_row_t *row)
{
    partition_t *partition = cdb_cube_get_partition(cube, insert_row_name(row));
    if (!partition)
        partition = cdb_cube_add_partition(cube, insert_row_name(row));
    return partition_insert_row(partition, row);
}

static void group_value_to_count_cleanup(void *value_to_count)
{
    htable_destroy(value_to_count);
}

htable_t *cdb_cube_pcount_from_to(cdb_cube *cube, char *from, char *to, filter_t *filter, char *group_by_column)
{
    size_t result_size = htable_size(cube->name_to_partition) * 2;

    bool partition_filter(void *key) {
        sds partition_name = key;
        bool is_within_range = true;
        if (from) is_within_range = is_within_range && strcoll(partition_name, from) >= 0;
        if (to) is_within_range = is_within_range && strcoll(partition_name, to) <= 0;
        return is_within_range;
    }

    htable_t *partition_to_results = NULL;
    if (!group_by_column) {
        /* Just counting? the htable with contain the counter */
        partition_to_results = htable_create(result_size, free);
        htable_for_each_filter(item, cube->name_to_partition, partition_filter) {
            sds partition_name = htable_key(item);
            partition_t *partition = htable_value(item);

            counter_t *count = cdb_malloc(sizeof(*count));
            *count = partition_count_filter(partition, filter);
            htable_put(partition_to_results, partition_name, count);
        }
    } else {
        /* Grouped counting? the htable with contain value to count mapping */
        partition_to_results = htable_create(result_size, group_value_to_count_cleanup);
        htable_for_each_filter(item, cube->name_to_partition, partition_filter) {
            sds partition_name = htable_key(item);
            partition_t *partition = htable_value(item);

            htable_t *group_value_to_count = partition_count_filter_grouped(partition, filter, group_by_column);
            htable_put(partition_to_results, partition_name, group_value_to_count);
        }
    }
    return partition_to_results;
}

static void value_set_cleanup(void *value_set)
{
    htable_destroy(value_set);
}

htable_t *cdb_cube_get_columns_to_value_set(cdb_cube *cube, char *from, char *to)
{
    bool partition_filter(void *key) {
        sds partition_name = key;
        bool is_within_range = true;
        if (from) is_within_range = is_within_range && strcoll(partition_name, from) >= 0;
        if (to) is_within_range = is_within_range && strcoll(partition_name, to) <= 0;
        return is_within_range;
    }

    /* TODO: setup destructors */
    htable_t *column_to_value_set = htable_create(htable_size(cube->name_to_partition) * 2, value_set_cleanup);

    htable_for_each_filter(item, cube->name_to_partition, partition_filter) {
        partition_t *partition = htable_value(item);
        partition_extend_column_value_set(partition, column_to_value_set);
    }

    return column_to_value_set;
}

void *cdb_cube_count_from_to(cdb_cube *cube, char *from, char *to, filter_t *filter, char *group_by_column)
{
    bool partition_filter(void *key) {
        sds partition_name = key;
        bool is_within_range = true;
        if (from) is_within_range = is_within_range && strcoll(partition_name, from) >= 0;
        if (to) is_within_range = is_within_range && strcoll(partition_name, to) <= 0;
        return is_within_range;
    }

    if (!group_by_column) {
        /* No grouping? Just return the counter. */
        counter_t *count = cdb_calloc(sizeof(*count));
        htable_for_each_filter(item, cube->name_to_partition, partition_filter) {
            partition_t *partition = htable_value(item);
            *count += partition_count_filter(partition, filter);
        }
        return count;
    } else {
        /* Grouping? Get value tables for each partition, merge them and return the result . */
        htable_t *value_to_count = htable_create(1024, free);
        htable_for_each_filter(item, cube->name_to_partition, partition_filter) {
            partition_t *partition = htable_value(item);
            htable_t *partition_value_to_count = partition_count_filter_grouped(partition, filter, group_by_column);
            defer { htable_destroy(partition_value_to_count); }

            htable_for_each(partition_item, partition_value_to_count) {
                char *value = htable_key(partition_item);
                counter_t *counter = htable_value(partition_item);
                if (!htable_has(value_to_count, value)) {
                    counter_t *counter_copy = cdb_calloc(sizeof(*counter_copy));
                    *counter_copy = *counter;
                    htable_put(value_to_count, value, counter_copy);
                } else {
                    counter_t *total_counter = htable_get(value_to_count, value);
                    *total_counter += *counter;
                }
            }
        }
        return value_to_count;
    }
}

char **cdb_cube_get_partition_names(cdb_cube *cube, size_t *partition_count)
{
    char **partition_names = cdb_malloc(sizeof(partition_names[0]) * htable_size(cube->name_to_partition));
    size_t i = 0;
    htable_for_each(item, cube->name_to_partition) {
        partition_names[i] = htable_key(item);
        i++;
    }
    *partition_count = i;
    return partition_names;
}

bool cdb_cube_has_partition(cdb_cube *cube, sds partition_name)
{
    return NULL != htable_get(cube->name_to_partition, partition_name);
}

bool cdb_cube_delete_partition_from_to(cdb_cube *cube, char *from, char *to)
{
    bool deleted = false;

    bool partition_filter(void *key) {
        sds partition_name = key;
        bool is_within_range = true;
        if (from) is_within_range = is_within_range && strcoll(partition_name, from) >= 0;
        if (to) is_within_range = is_within_range && strcoll(partition_name, to) <= 0;
        return is_within_range;
    }

    htable_for_each_filter(item, cube->name_to_partition, partition_filter) {
        char *key = htable_key(item);
        deleted = htable_del(cube->name_to_partition, key);
    }

    return deleted;
}
