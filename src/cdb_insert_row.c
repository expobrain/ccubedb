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

bool cdb_insert_row_parse_values(cdb_insert_row *row, sds column_to_value_list)
{
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

         if (2 != pair_tokens_len)
             return false;

         sds column = pair_tokens[0];
         sds value = pair_tokens[1];

         if (cdb_insert_row_has_column(row, column))
             return false;

         cdb_insert_row_add_column_value(row, column, value);
     }

     return true;
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

bool cdb_insert_row_has_column_value(cdb_insert_row *row, char *column_name, char *column_value)
{
    char *value = htable_get(row->column_to_value, column_name);
    return value != NULL && 0 == strcmp(value, column_value);
}

void cdb_insert_row_add_column_value(cdb_insert_row *row, char *column_name, char *column_value)
{
    htable_put(row->column_to_value, column_name, sdsnew(column_value));
}

sds cdb_insert_row_name(cdb_insert_row *row)
{
    return row->partition_name;
}
