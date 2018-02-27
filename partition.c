#include <stddef.h>
#include <stdlib.h>
#include <assert.h>
#include <stdbool.h>

#include "partition.h"
#include "slist.h"
#include "htable.h"
#include "cdb_alloc.h"

#define UNSPECIFIED_COLUMN_VALUE UINT16_MAX
#define UNKNOWN_FILTER_COLUMN_VALUE (UINT16_MAX - 1)

static value_id_t unknown_value_id = UNKNOWN_FILTER_COLUMN_VALUE;

typedef struct column_mapping_t column_mapping_t;
struct column_mapping_t {
    column_id_t id;
    htable_t *value_to_id;
};

struct partition_t {
    value_id_t **columns;
    size_t column_num;
    size_t row_num;
    htable_t *column_name_to_mapping;
    counter_t *counters;
};

static void column_mapping_init(column_mapping_t *mapping, column_id_t id)
{
    *mapping = (typeof(*mapping)){
        .id = id,
        .value_to_id = htable_create(1024, free),
    };
}

static column_mapping_t *column_mapping_create(column_id_t id)
{
    column_mapping_t *mapping = cdb_malloc(sizeof(column_mapping_t));
    column_mapping_init(mapping, id);
    return mapping;
}

static void column_mapping_destroy(column_mapping_t *mapping)
{
    htable_destroy(mapping->value_to_id);
    free(mapping);
}

static value_id_t *column_mapping_add_value(column_mapping_t *column_mapping, sds value)
{
    value_id_t *id = cdb_malloc(sizeof(*id));
    *id = htable_size(column_mapping->value_to_id);
    htable_put(column_mapping->value_to_id, value, id);
    return id;
}

static void mapping_cleanup(void *mapping)
{
    column_mapping_destroy(mapping);
}

void partition_init(partition_t *partition)
{
    *partition = (typeof(*partition)){
        .columns = NULL,
        .column_num = 0,
        .row_num = 0,
        .column_name_to_mapping = htable_create(1024, mapping_cleanup),
        .counters = NULL,
    };
}

partition_t *partition_create()
{
    partition_t *partition = cdb_malloc(sizeof(*partition));
    partition_init(partition);
    return partition;
}

void partition_destroy(partition_t *partition)
{
    htable_destroy(partition->column_name_to_mapping);

    for (size_t column_id = 0; column_id < partition->column_num; column_id++) {
        free(partition->columns[column_id]);
    }
    free(partition->columns);

    free(partition->counters);
    free(partition);
}

static column_mapping_t *partition_add_column_mapping(partition_t *partition, sds column_name)
{
    column_mapping_t *column_mapping =
        column_mapping_create(htable_size(partition->column_name_to_mapping));
    htable_put(partition->column_name_to_mapping, column_name, column_mapping);
    return column_mapping;
}

static void partition_add_column(partition_t *partition, sds column_name)
{
    column_mapping_t *mapping =
        partition_add_column_mapping(partition, column_name);
    partition->column_num++;
    partition->columns = cdb_realloc(partition->columns,
                                     sizeof(partition->columns[0]) * partition->column_num);
    partition->columns[mapping->id] =
        cdb_malloc(sizeof(partition->columns[mapping->id][0]) * partition->row_num);

    /* fill with default values */
    value_id_t *column = partition->columns[mapping->id];
    for (size_t r = 0; r > partition->row_num; r++) {
        column[r] = UNSPECIFIED_COLUMN_VALUE;
    }
}

static slist_t **partition_convert_filter(partition_t *partition, filter_t *filter)
{
    /* Initialize the row to search for */
    slist_t **values_to_look_for = cdb_malloc(sizeof(&values_to_look_for[0]) * partition->column_num);
    for (size_t c = 0; c < partition->column_num; c++ ) {
        values_to_look_for[c] = slist_create();
    }

    if (!filter) return values_to_look_for;

    /* Convert column name / column value strings into partition-specific ids */
    slist_for_each(node, filter->column_to_value_list) {
        column_value_pair *str_pair = slist_data(node);

        /* No mappings for the column? Abort */
        column_mapping_t *column_mapping = htable_get(partition->column_name_to_mapping, str_pair->column);
        if (!column_mapping)
            goto err;

        /* Unknown column value? Nothing is going to match it,use a special value to mark it */
        value_id_t *value_id_ptr = htable_get(column_mapping->value_to_id, str_pair->value);
        if (!value_id_ptr)
            value_id_ptr = &unknown_value_id;

        column_id_t column_id = column_mapping->id;
        slist_append(values_to_look_for[column_id], value_id_ptr);
    }

    return values_to_look_for;

err:
    for (size_t c = 0; c < partition->column_num; c++ ) {
        slist_destroy(values_to_look_for[c]);
    }
    free(values_to_look_for);
    return NULL;
}


static value_id_t *partition_convert_insert_row(partition_t *partition, insert_row_t *insert_row)
{
    /* Initialize the row to search for */
    value_id_t *values_to_look_for = cdb_malloc(sizeof(partition->columns[0][0]) * partition->column_num);
    for (size_t c = 0; c < partition->column_num; c++ ) {
        values_to_look_for[c] = UNSPECIFIED_COLUMN_VALUE;
    }

    /* Convert column name / column value strings into partition-specific ids */
    htable_for_each(node, insert_row->column_to_value) {
        char *column_name = htable_key(node);
        sds column_value = htable_value(node);

        column_mapping_t *column_mapping = htable_get(partition->column_name_to_mapping, column_name);
        if (!column_mapping) {
            partition_add_column(partition, column_name);
            column_mapping = htable_get(partition->column_name_to_mapping, column_name);
            assert(column_mapping);
        }

        value_id_t *value_id_ptr = htable_get(column_mapping->value_to_id, column_value);
        if (!value_id_ptr) {
            value_id_ptr = column_mapping_add_value(column_mapping, column_value);
            assert(value_id_ptr);
        }

        column_id_t column_id = column_mapping->id;
        values_to_look_for[column_id] = *value_id_ptr;
    }

    return values_to_look_for;
}

static inline bool is_row_matching_filter(partition_t *partition, size_t row_index, slist_t **values_to_look_for)
{
    for (size_t c = 0; c < partition->column_num; c++ ) {
        slist_t *column_values_to_look_for = values_to_look_for[c];

        /* No need to check columns that were not specified in the filters */
        if (0 == slist_size(column_values_to_look_for))
            continue;

        value_id_t *column = partition->columns[c];
        value_id_t column_row_value = column[row_index];

        /* If the column is specified and value do not match - skip the row  */
        bool any_value_matching = false;
        slist_for_each(node, column_values_to_look_for) {
            value_id_t *filter_column_value = slist_data(node);
            if (column_row_value == *filter_column_value) {
                any_value_matching = true;
                break;
            }
        }
        /* The filter was defined for the column and nothing matched - this means the row doesn't
         * match */
        if (!any_value_matching) {
            return false;
        }
    }
    return true;
}

counter_t partition_count_filter(partition_t *partition, filter_t *filter)
{
    counter_t total_count = 0;

    /* A list of value id lists instead of string values */
    slist_t **values_to_look_for =
        partition_convert_filter(partition, filter);

    /* Value mapping not found? Filtering meaningless  */
    if (!values_to_look_for)
        return total_count;

    defer {
        for (size_t c = 0; c < partition->column_num; c++ )
            slist_destroy(values_to_look_for[c]);
        free(values_to_look_for);
    }

    /* Now, walk all the rows while counting matching ones */
    for (size_t row_index = 0; row_index < partition->row_num; row_index++)
        if (is_row_matching_filter(partition, row_index, values_to_look_for))
            total_count += partition->counters[row_index];

    return total_count;
}

static char *get_column_value_id_value(column_mapping_t *column_mapping, const value_id_t column_value_id)
{
    /* TODO: this horror should be replaced by a proper constant time lookup */
    htable_for_each(item, column_mapping->value_to_id) {
        value_id_t *value_id_ptr = htable_value(item);
        if (column_value_id == *value_id_ptr) {
            return htable_key(item);
        }
    }
    return NULL;
}

htable_t *partition_count_filter_grouped(partition_t *partition, filter_t *filter, char *group_by_column)
{
    htable_t *group_value_to_count = htable_create(partition->column_num * 2, free);

    /* We need to know what column is used for grouping */
    column_mapping_t *group_column_mapping = htable_get(partition->column_name_to_mapping, group_by_column);
    if (!group_column_mapping)
        return group_value_to_count;
    column_id_t group_column_id = group_column_mapping->id;

    /* A list of value id lists instead of string values */
    slist_t **values_to_look_for =
        partition_convert_filter(partition, filter);

    /* Value mapping not found? Filtering meaningless  */
    if (!values_to_look_for)
        return group_value_to_count;

    defer {
        for (size_t c = 0; c < partition->column_num; c++ )
             slist_destroy(values_to_look_for[c]);
        free(values_to_look_for);
    }

    /* Now, walk all the rows while counting matching ones */
    for (size_t row_index = 0; row_index < partition->row_num; row_index++) {
        if (!is_row_matching_filter(partition, row_index, values_to_look_for))
            continue;

        value_id_t group_column_row_value_id = partition->columns[group_column_id][row_index];
        char *group_column_row_value =                                  \
            get_column_value_id_value(group_column_mapping, group_column_row_value_id);
        /* The value should always be there, it's a value in the partition, it should have a reverse
         * mapping */
        assert(group_column_row_value);

        if (!htable_has(group_value_to_count, group_column_row_value)) {
            counter_t *counter = cdb_calloc(sizeof(*counter));
            htable_put(group_value_to_count, group_column_row_value, counter);
        }

        counter_t *counter = htable_get(group_value_to_count, group_column_row_value);
        *counter += partition->counters[row_index];
    }


    return group_value_to_count;
}


static size_t partition_increase_row_count(partition_t *partition, size_t target_row, counter_t count)
{
    partition->counters[target_row] += count;
    return partition->counters[target_row];
}


static size_t partition_append_row(partition_t *partition, value_id_t *column_values)
{
    const size_t new_row_num = partition->row_num + 1;

    /* Resize all columns */
    for (size_t c = 0; c < partition->column_num; c++) {
        value_id_t *column = partition->columns[c];
        partition->columns[c] = cdb_realloc(column, sizeof(column[0]) * new_row_num);
        column = partition->columns[c];
        column[partition->row_num] = column_values[c];
    }

    /* Resize the counters column */
    counter_t *counters = partition->counters;
    partition->counters = cdb_realloc(counters, sizeof(counters[0]) * new_row_num);

    /* We now have one more proud row */
    size_t inserted_row_index = partition->row_num;
    partition->counters[inserted_row_index] = 0;
    partition->row_num = new_row_num;

    return inserted_row_index;
}

static inline bool is_row_matching_insert(partition_t *partition, size_t row_index, value_id_t *values_to_look_for)
{
    bool is_row_matching = true;

    for (size_t c = 0; c < partition->column_num; c++ ) {
        value_id_t *column = partition->columns[c];
        is_row_matching = true;
        value_id_t column_row_value = column[row_index];

        is_row_matching = is_row_matching && column_row_value == values_to_look_for[c];
        if (!is_row_matching)
            break;
    }

    return is_row_matching;
}

void partition_insert_row(partition_t *partition, insert_row_t *row)
{
    /* Get the ids instead of string values */
    value_id_t *values_to_look_for =
        partition_convert_insert_row(partition, row);
    defer { if (values_to_look_for) free(values_to_look_for); }

    /* Search for the insertion target */
    bool is_row_found = false;
    size_t target_row = 0;
    for (size_t row_index = 0; row_index < partition->row_num; row_index++)
        if (is_row_matching_insert(partition, row_index, values_to_look_for)) {
            target_row = row_index;
            is_row_found = true;
            break;
        }

    /* Not found? Insert the new row */
    if (!is_row_found)
        target_row = partition_append_row(partition, values_to_look_for);

    /* Increase the counter */
    partition_increase_row_count(partition, target_row, row->count);
}
