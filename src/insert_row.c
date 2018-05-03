#include <string.h>

#include "sds.h"
#include "insert_row.h"
#include "cdb_alloc.h"

insert_row_t *insert_row_create(const char *partition_name, counter_t count)
{
    insert_row_t *row = cdb_malloc(sizeof(insert_row_t));
    insert_row_init(row,partition_name,count);
    return row;
}

static void value_cleanup(void *sdsstr)
{
    sdsfree(sdsstr);
}

void insert_row_init(insert_row_t *row, const char *partition_name, counter_t count)
{
    *row = (typeof(*row)) {
        .partition_name = sdsnew(partition_name),
        .column_to_value = htable_create(32, value_cleanup),
        .count = count
    };
}

void insert_row_destroy(insert_row_t *row)
{
    sdsfree(row->partition_name);
    htable_destroy(row->column_to_value);
    free(row);
}

bool insert_row_has_column(insert_row_t *row, char *column_name)
{
    return htable_has(row->column_to_value, column_name);
}

void insert_row_add_column_value(insert_row_t *row, char *column_name, char *column_value)
{
    htable_put(row->column_to_value, column_name, sdsnew(column_value));
}

sds insert_row_name(insert_row_t *row)
{
    return row->partition_name;
}
