#ifndef CUBEDB_H
#define CUBEDB_H

#include "cdb_cube.h"

typedef struct cubedb_t cubedb_t;

cubedb_t *cubedb_create(void);
void cubedb_init(cubedb_t *cubedb);
void cubedb_destroy(cubedb_t *cubedb);

cdb_cube *cubedb_find_cube(cubedb_t *cubedb, const sds name);
bool cubedb_add_cube(cubedb_t *cubedb, sds cube_name, cdb_cube *cube);
bool cubedb_del_cube(cubedb_t *cubedb, sds cube_name);
char **cubedb_get_cube_names(cubedb_t *cubedb, size_t *cube_count);

/* TODO: delete a cube */


#endif //CUBEDB_H
