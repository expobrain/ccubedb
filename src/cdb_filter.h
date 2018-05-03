#ifndef CDB_FILTER_H
#define CDB_FILTER_H

#include "sds.h"
#include "slist.h"
#include "cdb_defs.h"

typedef struct cdb_filter cdb_filter;
struct cdb_filter {
    slist_t *column_to_value_list;
};

cdb_filter *cdb_filter_create();
cdb_filter *cdb_filter_parse_from_args(sds column_to_value_list, int *res);
void cdb_filter_init(cdb_filter *row);
void cdb_filter_destroy(cdb_filter *row);
void cdb_filter_add_column_value(cdb_filter *row, const char *column_name, const char *column_value);

#endif //C_FILTER_H
