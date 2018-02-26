#ifndef FILTER_H
#define FILTER_H

#include "sds.h"
#include "slist.h"
#include "defs.h"

typedef struct filter_t filter_t;
struct filter_t {
    slist_t *column_to_value_list;
};

filter_t *filter_create();
filter_t *filter_parse_from_args(sds column_to_value_list);
void filter_init(filter_t *row);
void filter_destroy(filter_t *row);
void filter_add_column_value(filter_t *row, const char *column_name, const char *column_value);


#endif //FILTER_H
