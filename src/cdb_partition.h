#ifndef CDB_PARTITION_H
#define CDB_PARTITION_H

#include "sds.h"
#include "cdb_defs.h"
#include "insert_row.h"
#include "filter.h"

typedef struct cdb_partition cdb_partition;

cdb_partition * partition_create();
void partition_init(cdb_partition *partition);
void partition_destroy(cdb_partition *partition);

counter_t partition_count_filter(cdb_partition *partition, filter_t *filter);
htable_t *partition_count_filter_grouped(cdb_partition *partition, filter_t *filter, char *group_by_column);
bool partition_insert_row(cdb_partition *partition, insert_row_t *row);
void partition_extend_column_value_set(cdb_partition *partition, htable_t *column_to_value_set);

#endif //CDB_PARTITION_H
