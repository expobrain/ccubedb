#include "filter.h"
#include "cdb_alloc.h"
#include "sds.h"


filter_t *filter_create()
{
    filter_t *row = cdb_malloc(sizeof(*row));
    filter_init(row);
    return row;
}

filter_t *filter_parse_from_args(sds column_to_value_list)
{
    filter_t *filter = filter_create();

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
            return NULL;

        sds column = pair_tokens[0];
        sds value = pair_tokens[1];

        filter_add_column_value(filter, column, value);
    }
    return filter;
}

void filter_init(filter_t *row)
{
    *row = (typeof(*row)) {
        .column_to_value_list = slist_create()
    };
}

void filter_destroy(filter_t *row)
{
    slist_for_each(node, row->column_to_value_list) {
        column_value_pair *pair = slist_data(node);
        column_value_pair_destroy(pair);
    }
    slist_destroy(row->column_to_value_list);
    free(row);
}

void filter_add_column_value(filter_t *row, const char *column_name, const char *column_value)
{
    column_value_pair *pair =
        column_value_pair_create(column_name, column_value);
    slist_append(row->column_to_value_list, pair);
}
