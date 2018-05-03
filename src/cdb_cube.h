#ifndef CDB_CUBE_H
#define CDB_CUBE_H

#include <stdint.h>

#include "sds.h"
#include "cdb_partition.h"
#include "insert_row.h"
#include "filter.h"
#include "htable.h"

typedef struct cdb_cube cdb_cube;

cdb_cube *cdb_cube_create(void);
void cdb_cube_init(cdb_cube *cube);
void cdb_cube_destroy(cdb_cube *cube);

htable_t *cdb_cube_pcount_from_to(cdb_cube *cube, char *from, char *to, filter_t *filter, char *group_by_column);
htable_t *cdb_cube_get_columns_to_value_set(cdb_cube *cube, char *from, char *to);
void *cdb_cube_count_from_to(cdb_cube *cube, char *from, char *to, filter_t *filter, char *group_by_column);
bool cdb_cube_insert_row(cdb_cube *cube, insert_row_t *row);
char **cdb_cube_get_partition_names(cdb_cube *cube, size_t *partition_count);
bool cdb_cube_has_partition(cdb_cube *cube, sds partition_name);
bool cdb_cube_delete_partition_from_to(cdb_cube *cube, char *from, char *to);

#endif //CDB_CUBE_H
