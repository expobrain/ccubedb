#ifndef CDB_CUBEDB_H
#define CDB_CUBEDB_H

#include "cdb_cube.h"

typedef struct cdb_cubedb cdb_cubedb;

typedef void cdb_cubedb_cube_visitor_function(sds cube_name, cdb_cube *cube);

cdb_cubedb *cdb_cubedb_create(void);
void cdb_cubedb_init(cdb_cubedb *cubedb);
void cdb_cubedb_destroy(cdb_cubedb *cubedb);

cdb_cube *cdb_cubedb_find_cube(cdb_cubedb *cubedb, const sds name);
bool cdb_cubedb_add_cube(cdb_cubedb *cubedb, sds cube_name, cdb_cube *cube);
bool cdb_cubedb_del_cube(cdb_cubedb *cubedb, sds cube_name);
char **cdb_cubedb_get_cube_names(cdb_cubedb *cubedb, size_t *cube_count);

void cdb_cubedb_for_each_cube(cdb_cubedb *cubedb, cdb_cubedb_cube_visitor_function cube_visitor);

/* TODO: delete a cube */


#endif //CDB_CUBEDB_H
