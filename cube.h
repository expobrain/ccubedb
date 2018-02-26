#ifndef CUBE_H
#define CUBE_H

#include <stdint.h>

#include "sds.h"
#include "partition.h"
#include "insert_row.h"
#include "filter.h"
#include "htable.h"

typedef struct cube_t cube_t;

cube_t *cube_create(void);
void cube_init(cube_t *cube);
void cube_destroy(cube_t *cube);

htable_t *cube_pcount_from_to(cube_t *cube, char *from, char *to, filter_t *filter, char *group_by_column);
void *cube_count_from_to(cube_t *cube, char *from, char *to, filter_t *filter, char *group_by_column);
void cube_insert_row(cube_t *cube, insert_row_t *row);
char **cube_get_partition_names(cube_t *cube, size_t *partition_count);
bool cube_has_partition(cube_t *cube, sds partition_name);
bool cube_delete_partition_from_to(cube_t *cube, char *from, char *to);

#endif //CUBE_H
