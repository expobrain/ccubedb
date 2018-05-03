#ifndef CDB_ALLOC_H
#define CDB_ALLOC_H

#include <stdio.h>
#include <stdlib.h>

static inline void cdb_oom(void)
{
    fputs("out of memory", stderr);
    abort();
}

static inline void *cdb_malloc(size_t size)
{
    void *res = malloc(size);
    if (!res) cdb_oom();
    return res;
}

static inline void *cdb_calloc(size_t size)
{
    void *res = calloc(1, size);
    if (!res) cdb_oom();
    return res;
}

static inline void *cdb_realloc(void *ptr, size_t size)
{
    void *res = realloc(ptr, size);
    if (!res) cdb_oom();
    return res;
}



#endif //CDB_ALLOC_H
