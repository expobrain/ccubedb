#ifndef HTABLE_H
#define HTABLE_H

#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <assert.h>

#include "cdb_alloc.h"

#define HTABLE_LOAD_FACTOR 0.7

#define htable_for_each(item, htable)                                   \
    for (htable_index_item_t (item) = { .i = 0, .pair = (htable)->table[0] }; (item).i < (htable)->table_size; (item).i++, (item).pair = (htable)->table[(item).i]) \
        if (!(item).pair.deleted && NULL != (item).pair.key)

#define htable_for_each_filter(item, htable, predicate) \
    htable_for_each((item), (htable))                   \
    if ((*(predicate))(htable_key((item))))

typedef struct htable_item_t htable_item_t;
struct htable_item_t {
    char *key;
    void *value;
    bool deleted;
};

typedef struct htable_index_item_t htable_index_item_t;
struct htable_index_item_t {
    size_t i;
    htable_item_t pair;
};

typedef struct htable_t htable_t;
struct htable_t {
    size_t table_size;
    size_t item_num;
    htable_item_t *table;
};

static inline uint64_t htable_hash(char *key)
{
    /* The famous djb2 algorithm by Dan Bernstein. */
    uint64_t hash = 5381;
    char c;
    while ((c = *key++))
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    return hash;
}

static inline void htable_reset_table(htable_t *table)
{
    for(size_t i = 0; i < table->table_size; i++) {
        table->table[i] = (typeof(table->table[0])) {
            .key = NULL,
            .value = NULL,
            .deleted = false
        };
    }
}

static inline htable_t *htable_create(size_t table_size)
{
    table_size = table_size > 1 ? table_size : 2u;

    htable_t *table = cdb_malloc(sizeof(*table));
    *table = (typeof(*table)) {
        .table_size = table_size,
        .item_num = 0,
        .table = cdb_malloc(sizeof(table->table[0]) * table_size)
    };

    htable_reset_table(table);

    return table;
}

static inline size_t htable_find_index(htable_t *table, char *key)
{
    uint64_t hash = htable_hash(key);
    size_t index = hash % table->table_size;
    while (NULL != table->table[index].key &&
           0 != strcmp(key, table->table[index].key)) {
        index++;
        index %= table->table_size;
    }
    return index;
}

static inline void htable_put_no_copy_no_resize(htable_t *table, char *key, char *value)
{
    size_t index = htable_find_index(table, key);
    table->table[index] = (typeof(table->table[index])) {
        .key = key,
        .value = value,
        .deleted = false
    };
    table->item_num++;
}

static inline void htable_resize(htable_t *table)
{
    htable_item_t *old_table = table->table;
    size_t old_size = table->table_size;

    table->table_size *= 2;
    table->table = cdb_malloc(table->table_size * sizeof(table->table[0]));
    table->item_num = 0;

    htable_reset_table(table);

    for (size_t i = 0; i < old_size; i++) {
        htable_item_t item = old_table[i];
        if (item.deleted) {
            free(item.key);
            continue;
        }
        if (NULL == item.key)
            continue;
        htable_put_no_copy_no_resize(table, item.key, item.value);
    }

    free(old_table);
}

static inline void *htable_del(htable_t *table, char *key)
{
    uint64_t hash = htable_hash(key);
    size_t index = hash % table->table_size;
    char *index_key = table->table[index].key;
    while (NULL != index_key) {
        if (!table->table[index].deleted && 0 == strcmp(index_key, key)) {
            table->table[index].deleted = true;
            void *value = table->table[index].value;
            table->table[index].value = NULL;
            table->item_num--;
            return value;
        }
        index++;
        index %= table->table_size;
        index_key = table->table[index].key;
    }
    return NULL;
}

static inline void *htable_put(htable_t *table, char *key, void *value)
{
    size_t index = htable_find_index(table, key);
    void *old_value = table->table[index].value;

    table->table[index].value = value;
    table->table[index].deleted = false;
    if (!table->table[index].key)
        table->table[index].key = strdup(key);

    table->item_num++;

    if ((double)table->item_num / table->table_size >= HTABLE_LOAD_FACTOR)
        htable_resize(table);

    return old_value;
}

static inline void *htable_get(htable_t *table, char *key)
{
    uint64_t hash = htable_hash(key);
    size_t index = hash % table->table_size;
    char *index_key = table->table[index].key;
    while (NULL != index_key) {
        if (!table->table[index].deleted && 0 == strcmp(table->table[index].key, key))
            return table->table[index].value;
        index++;
        index %= table->table_size;
        index_key = table->table[index].key;
    }
    return NULL;
}

static inline bool htable_has(htable_t *table, char *key)
{
    return NULL != htable_get(table, key);
}

static inline void htable_destroy(htable_t *table)
{
    for (size_t i = 0; i < table->table_size; i++) {
        char *key = table->table[i].key;
        if (key) free(table->table[i].key);
    }
    free(table->table);
    free(table);
}

static inline void *htable_value(htable_index_item_t item) {
    return item.pair.value;
}

static inline char *htable_key(htable_index_item_t item) {
    return item.pair.key;
}

static inline size_t htable_size(htable_t *table) {
    return table->item_num;
}

static inline size_t htable_table_size(htable_t *table) {
    return table->table_size;
}


#endif //HTABLE_H
