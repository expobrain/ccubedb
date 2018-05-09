#include "minunit.h"
#include "cdb_partition.h"

int tests_run = 0;

static char *test_empty()
{
    cdb_partition *partition = cdb_partition_create();
    mu_assert("Failed to create a partition", partition);
    defer { cdb_partition_destroy(partition); }
    return 0;
}

static char *test_for_each_simple()
{
    cdb_partition *partition = cdb_partition_create();
    defer { cdb_partition_destroy(partition); }

    {
        cdb_insert_row *row = cdb_insert_row_create("2018-02-02_21", 1);
        defer { cdb_insert_row_destroy(row); }

        cdb_insert_row_add_column_value(row, "screen_name", "214");

        cdb_partition_insert_row(partition, row);
        cdb_partition_insert_row(partition, row);
    }

    uint64_t row_count = 0;
    uint64_t value_count = 0;
    bool column_found = false;
    bool column_value_found = false;

    void row_visitor(cdb_insert_row *row)
    {
        row_count++;
        value_count += row->count;

        column_found = cdb_insert_row_has_column(row, "screen_name");
        column_value_found = cdb_insert_row_has_column_value(row, "screen_name", "214");
    }

    cdb_partition_for_each_row(partition, row_visitor);

    mu_assert("No column found", column_found);
    mu_assert("No column value found", column_value_found);
    mu_assert("Wrong row count", 1 == row_count);
    mu_assert("Wrong value count", 2 == value_count);

    return 0;
}

static char *all_tests()
{
    mu_run_test(test_empty);
    mu_run_test(test_for_each_simple);
    return 0;
}

int main(int argc, char **argv)
{
    /* Do not log anything */
    config = cdb_config_create(argc, argv);
    config->log_level = -1;

    char *result = all_tests();
    if (result != 0) {
        printf("%s\n", result);
    } else {
        printf("ALL TESTS PASSED: %s\n", __FILE__);
    }
    printf("Tests run: %d\n", tests_run);

    return result != 0;
}
