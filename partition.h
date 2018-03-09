#ifndef PARTITION_H
#define PARTITION_H

#include "sds.h"
#include "defs.h"
#include "insert_row.h"
#include "filter.h"

typedef struct partition_t partition_t;

partition_t * partition_create();
void partition_init(partition_t *partition);
void partition_destroy(partition_t *partition);

counter_t partition_count_filter(partition_t *partition, filter_t *filter);
htable_t *partition_count_filter_grouped(partition_t *partition, filter_t *filter, char *group_by_column);
bool partition_insert_row(partition_t *partition, insert_row_t *row);
void partition_extend_column_value_set(partition_t *partition, htable_t *column_to_value_set);

#endif //PARTITION_H
