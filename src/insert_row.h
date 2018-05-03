#ifndef INSERT_ROW_H
#define INSERT_ROW_H

#include "sds.h"
#include "htable.h"
#include "defs.h"

typedef struct insert_row_t insert_row_t;
struct insert_row_t {
    sds partition_name;

    htable_t *column_to_value;

    counter_t count;
};

insert_row_t *insert_row_create(const char *partition_name, counter_t count);
void insert_row_init(insert_row_t *row, const char *partition_name, counter_t count);
void insert_row_destroy(insert_row_t *row);
bool insert_row_has_column(insert_row_t *row, char *column_name);
void insert_row_add_column_value(insert_row_t *row, char *column_name, char *column_value);
sds insert_row_name(insert_row_t *row);

#endif //INSERT_ROW_H
