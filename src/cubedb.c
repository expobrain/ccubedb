#include <stdlib.h>
#include <stddef.h>
#include <string.h>

#include "cubedb.h"
#include "htable.h"
#include "cdb_alloc.h"

struct cubedb_t {
    htable_t *name_to_cube;
};

cubedb_t *cubedb_create()
{
    cubedb_t *cubedb = cdb_malloc(sizeof(*cubedb));
    cubedb_init(cubedb);
    return cubedb;
}

static void cube_cleanup(void *cube)
{
    cdb_cube_destroy(cube);
}

void cubedb_init(cubedb_t *cubedb)
{
    *cubedb = (typeof(*cubedb)){
        .name_to_cube = htable_create(1024, cube_cleanup)
    };
}

void cubedb_destroy(cubedb_t *cubedb)
{
    htable_destroy(cubedb->name_to_cube);
    free(cubedb);
}

cdb_cube *cubedb_find_cube(cubedb_t *cubedb, char *name)
{
    return htable_get(cubedb->name_to_cube, name);
}

bool cubedb_add_cube(cubedb_t *cubedb, sds cube_name, cdb_cube *cube)
{
    htable_t *cube_table = cubedb->name_to_cube;
    if (htable_has(cube_table, cube_name))
        return false;
    htable_put(cube_table, cube_name, cube);
    return true;
}

bool cubedb_del_cube(cubedb_t *cubedb, sds cube_name)
{
    htable_t *cube_table = cubedb->name_to_cube;
    cdb_cube *cube = htable_get(cube_table, cube_name);
    if (!cube) return false;
    htable_del(cube_table, cube_name);
    return true;
}


char **cubedb_get_cube_names(cubedb_t *cubedb, size_t *cube_count)
{
    char **cube_names = cdb_malloc(sizeof(cube_names[0]) * htable_size(cubedb->name_to_cube));
    size_t i = 0;
    htable_for_each(item, cubedb->name_to_cube) {
        cube_names[i] = htable_key(item);
        i++;
    }
    *cube_count = i;
    return cube_names;
}
