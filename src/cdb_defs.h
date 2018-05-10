#ifndef CDB_DEFS_H
#define CDB_DEFS_H

#include <stdint.h>
#include <stdlib.h>

#include "sds.h"
#include "cdb_alloc.h"

typedef uint8_t column_id_t;
typedef uint16_t value_id_t;
typedef uint64_t counter_t;

#define VALUE_ID_UNKNOWN UINT16_MAX
#define VALUE_ID_FILTER_UNSPECIFIED (UINT16_MAX - 1)
#define VALUE_ID_MAX (UINT16_MAX - 2)

#define defer_(x) do{}while(0);                                         \
    auto void _dtor1_##x();                                             \
    auto void _dtor2_##x();                                             \
    int __attribute__((cleanup(_dtor2_##x))) _dtorV_##x=0;              \
    void _dtor2_##x(){ if(_dtorV_##x!=0) return _dtor1_##x(); };        \
    _dtorV_##x=42;                                                      \
    void _dtor1_##x()
#define defer__(x) defer_(x)
#define defer defer__(__COUNTER__)

#define RECEIVE_BUFSIZE 512
#define MAX_QUERY_SIZE 4096

typedef struct column_value_pair column_value_pair;
struct column_value_pair {
    sds column;
    sds value;
};

static inline void column_value_pair_init(column_value_pair *pair, const char *column, const char *value)
{
    *pair = (typeof(*pair)) {
        .column = sdsnew(column),
        .value = sdsnew(value),
    };
}

static inline column_value_pair *column_value_pair_create(const char *column, const char *value)
{
    column_value_pair *pair = cdb_malloc(sizeof(*pair));
    column_value_pair_init(pair, column, value);
    return pair;
}

static inline void column_value_pair_destroy(column_value_pair *pair)
{
    sdsfree(pair->column);
    sdsfree(pair->value);
    free(pair);
}

#endif //CDB_DEFS_H
