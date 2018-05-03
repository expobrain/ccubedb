#ifndef SLIST_H
#define SLIST_H

#include <stddef.h>
#include <stdlib.h>
#include <stdbool.h>
#include <assert.h>

#include "cdb_alloc.h"

#define slist_for_each(node, list)                                      \
    for (slist_node_t *(node) = (list)->head; (node); (node) = (node)->next)

#define slist_for_each_filter(node, list, predicate)    \
    slist_for_each(node, list)                          \
    if ((*(predicate))(slist_data(node)))

typedef struct slist_t slist_t;

typedef struct slist_node_t slist_node_t;

struct slist_t {
    size_t size;
    slist_node_t *head;
    slist_node_t *tail;
};

struct slist_node_t {
    slist_node_t *next;
    void *data;
};

static inline slist_t *slist_create(void)
{
    slist_t *list = cdb_malloc(sizeof(slist_t));
    *list = (slist_t){
        .size = 0,
        .head = NULL,
        .tail = NULL,
    };
    return list;
}

static inline void slist_destroy(slist_t *list)
{
    slist_node_t *node = list->head;
    while(NULL != node){
        slist_node_t *next_node = node->next;
        free(node);
        node = next_node;
    }
    free(list);
}

static inline slist_node_t *slist_prepend(slist_t *list, void *data)
{
    slist_node_t *node = cdb_malloc(sizeof(*list));
    *node = (typeof(*node)){
        .next = list->head,
        .data = data
    };
    list->head = node;
    if (!list->tail)
        list->tail = node;
    list->size++;
    return node;
}

static inline slist_node_t *slist_append(slist_t *list, void *data)
{
    slist_node_t *new_node = cdb_malloc(sizeof(*list));
    *new_node = (typeof(*new_node)){
        .next = NULL,
        .data = data
    };
    if (!list->tail) {
        assert(!list->tail);
        list->head = new_node;
        list->tail = new_node;
    } else {
        list->tail->next = new_node;
        list->tail = new_node;
    }

    list->size++;
    return new_node;
}

static inline void *slist_data(slist_node_t *node)
{
    return node->data;
}

static inline size_t slist_size(slist_t *list)
{
    return list->size;
}


static inline void *slist_head(slist_t *list)
{
    if (!slist_size(list))
        return NULL;
    return list->head->data;
}

static inline void *slist_tail(slist_t *list)
{
    if (!slist_size(list))
        return NULL;
    return list->tail->data;
}

static inline void *slist_pop_head(slist_t *list)
{
    if (!slist_size(list))
        return NULL;
    slist_node_t *head = list->head;
    void *head_data = head->data;
    if (head == list->tail)
        list->tail = NULL;
    list->head = head->next;
    list->size--;
    free(head);
    return head_data;
}

static inline void *slist_delete_single_if(slist_t *list, bool predicate(void *data))
{
    for (slist_node_t *this = list->head, *prev = NULL; this; prev = this, this = this->next) {
        if (predicate(this->data)) {
            void *data = this->data;

            if (this == list->head)
                list->head = this->next;
            if (this == list->tail)
                list->tail = prev;
            if (prev)
                prev->next = this->next;

            free(this);
            list->size--;
            return data;
        }
    }
    return NULL;
}

static inline void *slist_find(slist_t *list, bool predicate(void *data))
{
    slist_for_each(node, list) {
        void *data = slist_data(node);
        if (predicate(data))
            return data;
    }
    return NULL;
}

#endif //SLIST_H
