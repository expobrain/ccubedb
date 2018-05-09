#ifndef CDB_PARTITION_H
#define CDB_PARTITION_H

#include "sds.h"
#include "cdb_defs.h"
#include "cdb_insert_row.h"
#include "cdb_filter.h"

typedef struct cdb_partition cdb_partition;

typedef void cdb_row_visitor_function(cdb_insert_row *row, void *visitor_state);

cdb_partition * cdb_partition_create();
void cdb_partition_init(cdb_partition *partition);
void cdb_partition_destroy(cdb_partition *partition);

counter_t cdb_partition_count_filter(cdb_partition *partition, cdb_filter *filter);
htable_t *cdb_partition_count_filter_grouped(cdb_partition *partition, cdb_filter *filter, char *group_by_column);
bool cdb_partition_insert_row(cdb_partition *partition, cdb_insert_row *row);
void cdb_partition_extend_column_value_set(cdb_partition *partition, htable_t *column_to_value_set);

void cdb_partition_for_each_row(cdb_partition *partition, cdb_row_visitor_function visitor, void *visitor_state);

#endif //CDB_PARTITION_H
