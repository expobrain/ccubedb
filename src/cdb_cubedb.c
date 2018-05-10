#include <stdlib.h>
#include <stddef.h>
#include <string.h>

#include "cdb_cubedb.h"
#include "htable.h"
#include "cdb_alloc.h"

struct cdb_cubedb {
    htable_t *name_to_cube;
};

cdb_cubedb *cdb_cubedb_create()
{
    cdb_cubedb *cubedb = cdb_malloc(sizeof(*cubedb));
    cdb_cubedb_init(cubedb);
    return cubedb;
}

static void cube_cleanup(void *cube)
{
    cdb_cube_destroy(cube);
}

void cdb_cubedb_init(cdb_cubedb *cubedb)
{
    *cubedb = (typeof(*cubedb)){
        .name_to_cube = htable_create(1024, cube_cleanup)
    };
}

void cdb_cubedb_destroy(cdb_cubedb *cubedb)
{
    htable_destroy(cubedb->name_to_cube);
    free(cubedb);
}

cdb_cube *cdb_cubedb_find_cube(cdb_cubedb *cubedb, char *name)
{
    return htable_get(cubedb->name_to_cube, name);
}

bool cdb_cubedb_add_cube(cdb_cubedb *cubedb, sds cube_name, cdb_cube *cube)
{
    htable_t *cube_table = cubedb->name_to_cube;
    if (htable_has(cube_table, cube_name))
        return false;
    htable_put(cube_table, cube_name, cube);
    return true;
}

bool cdb_cubedb_del_cube(cdb_cubedb *cubedb, sds cube_name)
{
    htable_t *cube_table = cubedb->name_to_cube;
    cdb_cube *cube = htable_get(cube_table, cube_name);
    if (!cube) return false;
    htable_del(cube_table, cube_name);
    return true;
}


char **cdb_cubedb_get_cube_names(cdb_cubedb *cubedb, size_t *cube_count)
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

void cdb_cubedb_for_each_cube(cdb_cubedb *cubedb, cdb_cubedb_cube_visitor_function cube_visitor)
{
    htable_for_each(item, cubedb->name_to_cube) {
        sds cube_name = htable_key(item);
        cdb_cube *cube = htable_value(item);

        cube_visitor(cube_name, cube);
    }
}
