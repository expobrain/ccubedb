#include <string.h>

#include "cdb_filter.h"
#include "cdb_alloc.h"
#include "sds.h"


cdb_filter *cdb_filter_create()
{
    cdb_filter *row = cdb_malloc(sizeof(*row));
    cdb_filter_init(row);
    return row;
}

cdb_filter *cdb_filter_parse_from_args(sds column_to_value_list, int *res)
{
    *res = 0;
    if (0 == strcmp("null", column_to_value_list))
        return NULL;

    cdb_filter *filter = cdb_filter_create();

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

        if (2 != pair_tokens_len) {
            *res = -1;
            return NULL;
        }

        sds column = pair_tokens[0];
        sds value = pair_tokens[1];

        cdb_filter_add_column_value(filter, column, value);
    }
    return filter;
}

void cdb_filter_init(cdb_filter *row)
{
    *row = (typeof(*row)) {
        .column_to_value_list = slist_create()
    };
}

void cdb_filter_destroy(cdb_filter *row)
{
    slist_for_each(node, row->column_to_value_list) {
        column_value_pair *pair = slist_data(node);
        column_value_pair_destroy(pair);
    }
    slist_destroy(row->column_to_value_list);
    free(row);
}

void cdb_filter_add_column_value(cdb_filter *row, const char *column_name, const char *column_value)
{
    column_value_pair *pair =
        column_value_pair_create(column_name, column_value);
    slist_prepend(row->column_to_value_list, pair);
}
