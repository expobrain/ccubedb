#include <string.h>

#include "sds.h"
#include "cdb_insert_row.h"
#include "cdb_alloc.h"

cdb_insert_row *cdb_insert_row_create(const char *partition_name, counter_t count)
{
    cdb_insert_row *row = cdb_malloc(sizeof(cdb_insert_row));
    cdb_insert_row_init(row,partition_name,count);
    return row;
}

static void value_cleanup(void *sdsstr)
{
    sdsfree(sdsstr);
}

void cdb_insert_row_init(cdb_insert_row *row, const char *partition_name, counter_t count)
{
    *row = (typeof(*row)) {
        .partition_name = sdsnew(partition_name),
        .column_to_value = htable_create(32, value_cleanup),
        .count = count
    };
}

void cdb_insert_row_destroy(cdb_insert_row *row)
{
    sdsfree(row->partition_name);
    htable_destroy(row->column_to_value);
    free(row);
}

bool cdb_insert_row_has_column(cdb_insert_row *row, char *column_name)
{
    return htable_has(row->column_to_value, column_name);
}

void cdb_insert_row_add_column_value(cdb_insert_row *row, char *column_name, char *column_value)
{
    htable_put(row->column_to_value, column_name, sdsnew(column_value));
}

sds cdb_insert_row_name(cdb_insert_row *row)
{
    return row->partition_name;
}
