#include <stddef.h>
#include <stdlib.h>
#include <assert.h>
#include <stdbool.h>

#include "cdb_log.h"
#include "cdb_partition.h"
#include "slist.h"
#include "htable.h"
#include "cdb_alloc.h"
#include "khash.h"
#include "cdb_defs.h"

static value_id_t unknown_value_id = VALUE_ID_FILTER_UNSPECIFIED;

static inline uint64_t value_array_hash(const value_id_t *value_array);
static inline bool value_array_equal(const value_id_t *left_value_array, const value_id_t *right_value_array);
KHASH_INIT(value_to_row, value_id_t *, size_t, 1, value_array_hash, value_array_equal);
KHASH_MAP_INIT_INT(id_to_value, char *);

typedef struct column_mapping_t {
    column_id_t id;
    htable_t *value_to_id;
    khash_t(id_to_value) *id_to_value;
} column_mapping_t;

struct cdb_partition {
    value_id_t **columns;
    size_t column_num;
    khash_t(value_to_row) *value_to_row;
    size_t row_num;
    htable_t *column_name_to_mapping;
    counter_t *counters;
};

static void column_mapping_init(column_mapping_t *mapping, column_id_t id)
{
    *mapping = (typeof(*mapping)){
        .id = id,
        .value_to_id = htable_create(1024, free),
        .id_to_value = kh_init(id_to_value)
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
    char *value = NULL;
    kh_foreach_value(mapping->id_to_value, value, {
            free(value);
        });
    kh_destroy(id_to_value, mapping->id_to_value);
    free(mapping);
}

static inline bool column_mapping_can_add_value(column_mapping_t *column_mapping, sds column_value)
{
    /* The value is already there? it's ok, no need to add anything anyways */
    if (htable_has(column_mapping->value_to_id, column_value))
        return true;

    /* Otherwise check if the number of values is below  */
    return htable_size(column_mapping->value_to_id) <= VALUE_ID_MAX;
}

static value_id_t *column_mapping_add_value(column_mapping_t *column_mapping, sds value)
{
    value_id_t *id = cdb_malloc(sizeof(*id));
    *id = htable_size(column_mapping->value_to_id);
    htable_put(column_mapping->value_to_id, value, id);

    int ret;
    khint_t key = kh_put(id_to_value, column_mapping->id_to_value, *id, &ret);
    kh_value(column_mapping->id_to_value, key) = strdup(value);

    return id;
}

static void mapping_cleanup(void *mapping)
{
    column_mapping_destroy(mapping);
}

void cdb_partition_init(cdb_partition *partition)
{
    *partition = (typeof(*partition)){
        .columns = NULL,
        .column_num = 0,
        .value_to_row = kh_init(value_to_row),
        .row_num = 0,
        .column_name_to_mapping = htable_create(1024, mapping_cleanup),
        .counters = NULL,
    };
}

cdb_partition *cdb_partition_create()
{
    cdb_partition *partition = cdb_malloc(sizeof(*partition));
    cdb_partition_init(partition);
    return partition;
}

void cdb_partition_destroy(cdb_partition *partition)
{
    htable_destroy(partition->column_name_to_mapping);

    for (size_t column_id = 0; column_id < partition->column_num; column_id++) {
        free(partition->columns[column_id]);
    }
    free(partition->columns);

    value_id_t *key = NULL;
    kh_foreach_key(partition->value_to_row, key, {
            free(key);
        });

    kh_destroy(value_to_row, partition->value_to_row);

    free(partition->counters);
    free(partition);
}

static column_mapping_t *cdb_partition_add_column_mapping(cdb_partition *partition, sds column_name)
{
    column_mapping_t *column_mapping =
        column_mapping_create(htable_size(partition->column_name_to_mapping));
    htable_put(partition->column_name_to_mapping, column_name, column_mapping);
    return column_mapping;
}

static void cdb_partition_add_column(cdb_partition *partition, sds column_name)
{
    column_mapping_t *mapping =
        cdb_partition_add_column_mapping(partition, column_name);
    partition->column_num++;
    partition->columns = cdb_realloc(partition->columns,
                                     sizeof(partition->columns[0]) * partition->column_num);
    partition->columns[mapping->id] =
        cdb_malloc(sizeof(partition->columns[mapping->id][0]) * partition->row_num);

    /* fill with default values */
    value_id_t *column = partition->columns[mapping->id];
    for (size_t r = 0; r > partition->row_num; r++)
        column[r] = VALUE_ID_UNKNOWN;
}

static slist_t **convert_filter(cdb_partition *partition, cdb_filter *filter)
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
        slist_prepend(values_to_look_for[column_id], value_id_ptr);
    }

    return values_to_look_for;

err:
    for (size_t c = 0; c < partition->column_num; c++ ) {
        slist_destroy(values_to_look_for[c]);
    }
    free(values_to_look_for);
    return NULL;
}


static value_id_t *convert_insert_row(cdb_partition *partition, cdb_insert_row *insert_row)
{
    /* Make sure all the columns and values are present in the partition mappings */
    htable_for_each(node, insert_row->column_to_value) {
        char *column_name = htable_key(node);
        sds column_value = htable_value(node);

        column_mapping_t *column_mapping = htable_get(partition->column_name_to_mapping, column_name);
        if (!column_mapping) {
            cdb_partition_add_column(partition, column_name);
            column_mapping = htable_get(partition->column_name_to_mapping, column_name);
            assert(column_mapping);
        }

        value_id_t *value_id_ptr = htable_get(column_mapping->value_to_id, column_value);
        if (!value_id_ptr) {
            value_id_ptr = column_mapping_add_value(column_mapping, column_value);
            assert(value_id_ptr);
        }
    }

    /* Now, initialize the row to search for */
    value_id_t *values_to_insert =
        cdb_malloc(sizeof(partition->columns[0][0]) * (partition->column_num + 1));
    values_to_insert[0] = partition->column_num;

    for (size_t c = 0; c < partition->column_num; c++)
        values_to_insert[1 + c] = VALUE_ID_UNKNOWN;

    /* Convert column name / column value strings into partition-specific ids */
    htable_for_each(node, insert_row->column_to_value) {
        char *column_name = htable_key(node);
        sds column_value = htable_value(node);

        column_mapping_t *column_mapping = htable_get(partition->column_name_to_mapping, column_name);
        value_id_t *value_id_ptr = htable_get(column_mapping->value_to_id, column_value);
        assert(column_mapping);
        assert(value_id_ptr);

        column_id_t column_id = column_mapping->id;
        values_to_insert[column_id + 1] = *value_id_ptr;
    }

    return values_to_insert;
}

static inline bool is_row_matching_filter(cdb_partition *partition, size_t row_index, slist_t **values_to_look_for)
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

counter_t cdb_partition_count_filter(cdb_partition *partition, cdb_filter *filter)
{
    counter_t total_count = 0;

    /* A list of value id lists instead of string values */
    slist_t **values_to_look_for =
        convert_filter(partition, filter);

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
    khint_t key = kh_get(id_to_value, column_mapping->id_to_value, column_value_id);
    bool is_found = key != kh_end(column_mapping->id_to_value);
    if (!is_found)
        return NULL;
    char *value = kh_value(column_mapping->id_to_value, key);
    return value;
}

htable_t *cdb_partition_count_filter_grouped(cdb_partition *partition, cdb_filter *filter, char *group_by_column)
{
    htable_t *group_value_to_count = htable_create(partition->column_num * 2, free);

    /* We need to know what column is used for grouping */
    column_mapping_t *group_column_mapping = htable_get(partition->column_name_to_mapping, group_by_column);
    if (!group_column_mapping)
        return group_value_to_count;
    column_id_t group_column_id = group_column_mapping->id;

    /* A list of value id lists instead of string values */
    slist_t **values_to_look_for =
        convert_filter(partition, filter);

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
        char *group_column_row_value =
            get_column_value_id_value(group_column_mapping, group_column_row_value_id);
        /* The value should always be there, it's a value in the partition, it should have a reverse
         * mapping */
        assert(group_column_row_value);

        counter_t *counter = htable_get(group_value_to_count, group_column_row_value);
        if (!counter) {
            counter = cdb_calloc(sizeof(*counter));
            htable_put(group_value_to_count, group_column_row_value, counter);
        }

        *counter += partition->counters[row_index];
    }


    return group_value_to_count;
}


static size_t cdb_partition_increase_row_count(cdb_partition *partition, size_t target_row, counter_t count)
{
    partition->counters[target_row] += count;
    return partition->counters[target_row];
}


static size_t cdb_partition_append_row(cdb_partition *partition, value_id_t *values)
{
    const size_t new_row_num = partition->row_num + 1;

    /* Resize all columns */
    for (size_t c = 0; c < partition->column_num; c++) {
        value_id_t *column = partition->columns[c];
        partition->columns[c] = cdb_realloc(column, sizeof(column[0]) * new_row_num);
        column = partition->columns[c];
        column[partition->row_num] = values[c];
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

static inline uint64_t value_array_hash(const value_id_t *value_array)
{
    size_t value_array_size = value_array[0] + 1u;
    uint64_t hash = 1;
    for (size_t i = 0; i < value_array_size; i++)
        hash = hash * 31 + value_array[i];
    return hash;
}

static inline bool value_array_equal(const value_id_t *left_value_array, const value_id_t *right_value_array)
{
    size_t array_length = left_value_array[0] + 1u;
    size_t array_size = array_length * sizeof(left_value_array[0]);
    return 0 == memcmp(left_value_array, right_value_array, array_size);
}

static inline bool cdb_partition_can_insert_row(const cdb_partition *partition, const cdb_insert_row *row)
{
    /* Go through rows inserted and check that all columns can accept new values */
    htable_for_each(node, row->column_to_value) {
        char *column_name = htable_key(node);
        sds column_value = htable_value(node);

        /* Mapping not present? it can definitely be added */
        column_mapping_t *mapping = htable_get(partition->column_name_to_mapping, column_name);
        if (!mapping)
            continue;

        /* Now, see if the value can be added  */
        if (!column_mapping_can_add_value(mapping, column_value)) {
            log_warn(
                "Cannot add %s to %s because of too many values already present",
                column_value, column_name
            );
            return false;
        }
    }
    return true;
}

bool cdb_partition_insert_row(cdb_partition *partition, cdb_insert_row *row)
{
    /* Make sure the row can be added at all */
    if (!cdb_partition_can_insert_row(partition, row))
        return false;

    /* Get the ids instead of string values (array size + values in the array) */
    value_id_t *values_to_insert = convert_insert_row(partition, row);

    /* Search for the insertion target */
    khint_t key = kh_get(value_to_row, partition->value_to_row, values_to_insert);
    bool is_row_found = key != kh_end(partition->value_to_row);

    size_t target_row = 0;
    if (!is_row_found) {
        /* Row not found? append the row, register it's values in the mapping */
        int ret;
        key = kh_put(value_to_row, partition->value_to_row, values_to_insert, &ret);
        target_row = cdb_partition_append_row(partition, values_to_insert + 1);
        kh_value(partition->value_to_row, key) = target_row;
    } else {
        /* Found the row? This is where we increase the counter */
        target_row = kh_value(partition->value_to_row, key);
        free(values_to_insert);
    }

    cdb_partition_increase_row_count(partition, target_row, row->count);
    return true;
}

void cdb_partition_extend_column_value_set(cdb_partition *partition, htable_t *column_to_value_set)
{
    htable_for_each(item, partition->column_name_to_mapping) {
        char *column_name = htable_key(item);

        htable_t *value_set = htable_get(column_to_value_set, column_name);
        if (!value_set) {
            value_set = htable_create(512, NULL);
            htable_put(column_to_value_set, column_name, value_set);
        }

        column_mapping_t *column_mapping = htable_value(item);
        htable_for_each(value_item, column_mapping->value_to_id) {
            char *column_value = htable_key(value_item);
            htable_put(value_set, column_value, NULL);
        }
    }
}

void cdb_partition_for_each_row(cdb_partition *partition, cdb_partition_row_visitor_function visitor)
{
    for (size_t row_i = 0; row_i < partition->row_num; row_i++ ) {
        counter_t counter = partition->counters[row_i];
        cdb_insert_row * row = cdb_insert_row_create(NULL, counter);

        defer { cdb_insert_row_destroy(row); }

        visitor(row);
    }
}
