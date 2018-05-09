#ifndef CDB_INSERT_ROW_H
#define CDB_INSERT_ROW_H

#include "sds.h"
#include "htable.h"
#include "cdb_defs.h"

typedef struct cdb_insert_row {
    sds partition_name;

    htable_t *column_to_value;

    counter_t count;
} cdb_insert_row;

cdb_insert_row *cdb_insert_row_create(const char *partition_name, counter_t count);

bool cdb_insert_row_parse_values(cdb_insert_row *row, sds column_to_value_list);

void cdb_insert_row_init(cdb_insert_row *row, const char *partition_name, counter_t count);

void cdb_insert_row_destroy(cdb_insert_row *row);

bool cdb_insert_row_has_column(cdb_insert_row *row, char *column_name);

void cdb_insert_row_add_column_value(cdb_insert_row *row, char *column_name, char *column_value);

sds cdb_insert_row_name(cdb_insert_row *row);

#endif //CDB_INSERT_ROW_H
