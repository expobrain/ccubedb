#include <stdio.h>
#include <stdlib.h>

#include "minunit.h"
#include "cdb_cube.h"
#include "cdb_config.h"
#include "cdb_partition.h"
#include "cdb_defs.h"
#include "htable.h"

int tests_run = 0;

static char *test_empty()
{
    cdb_cube *cube = cdb_cube_create();
    defer { cdb_cube_destroy(cube); }

    counter_t *count = cdb_cube_count_from_to(cube,"part0","part9999", NULL, NULL);
    defer { free(count); }

    mu_assert("There is nothing in the cube and count != 0", 0 == *count);
    return 0;
}

static char *test_multiple_inserts()
{
    cdb_cube *cube = cdb_cube_create();
    defer { cdb_cube_destroy(cube); }

    /* Do a lot if comlicated inserts */
    for (size_t i = 0; i < 100; i++) {
        {
            /* screen_name=214&gender=1&brand=2&country_id=65&platform=3&app_version=5.39.0 2*/

            insert_row_t *row = insert_row_create("2018-02-02_21", 2);
            defer { insert_row_destroy(row); }

            insert_row_add_column_value(row, "screen_name", "214");
            insert_row_add_column_value(row, "gender", "1");
            insert_row_add_column_value(row, "brand", "2");
            insert_row_add_column_value(row, "country_id", "65");
            insert_row_add_column_value(row, "platform", "3");
            insert_row_add_column_value(row, "app_version", "5.39.0");

            cdb_cube_insert_row(cube, row);
        }

        {
            /* screen_name=217&gender=1&brand=2&country_id=48&platform=3&app_version=5.39.0 3*/

            insert_row_t *row = insert_row_create("2018-02-02_21", 3);
            defer { insert_row_destroy(row); }

            insert_row_add_column_value(row, "screen_name", "217");
            insert_row_add_column_value(row, "gender", "1");
            insert_row_add_column_value(row, "brand", "2");
            insert_row_add_column_value(row, "country_id", "48");
            insert_row_add_column_value(row, "platform", "3");
            insert_row_add_column_value(row, "app_version", "5.39.0");

            cdb_cube_insert_row(cube, row);
        }

        {
            /* screen_name=8&gender=0&brand=2&country_id=66&platform=3&app_version=5.45.0 1 */

            insert_row_t *row = insert_row_create("2018-02-02_21", 1);
            defer { insert_row_destroy(row); }

            insert_row_add_column_value(row, "screen_name", "8");
            insert_row_add_column_value(row, "gender", "0");
            insert_row_add_column_value(row, "brand", "2");
            insert_row_add_column_value(row, "country_id", "66");
            insert_row_add_column_value(row, "platform", "3");
            insert_row_add_column_value(row, "app_version", "5.45.0");

            cdb_cube_insert_row(cube, row);
        }

        {
            /* screen_name=123&gender=2&brand=2&country_id=64&platform=2&app_version=6.212.0 1030 */

            insert_row_t *row = insert_row_create("2018-02-02_21", 1030);
            defer { insert_row_destroy(row); }

            insert_row_add_column_value(row, "screen_name", "123");
            insert_row_add_column_value(row, "gender", "2");
            insert_row_add_column_value(row, "brand", "2");
            insert_row_add_column_value(row, "country_id", "64");
            insert_row_add_column_value(row, "platform", "2");
            insert_row_add_column_value(row, "app_version", "6.212.0");

            cdb_cube_insert_row(cube, row);
        }
    }

    return 0;
}

static char *test_count_from_to_single_row()
{
    cdb_cube *cube = cdb_cube_create();
    defer { cdb_cube_destroy(cube); }

    insert_row_t *row = insert_row_create("part1001", 3);
    cdb_cube_insert_row(cube, row);
    defer { insert_row_destroy(row); }

    {
        counter_t *count = cdb_cube_count_from_to(cube,"part0000","part1000", NULL, NULL);
        defer { free(count); }
        mu_assert("Counted a row not from the specified interval", 0 == *count);
    }

    {
        counter_t *count = cdb_cube_count_from_to(cube,"part0000","part1001", NULL, NULL);
        defer { free(count); }
        mu_assert("Couldn't find the row!", 3 == *count);
    }

    return 0;
}

static char *test_count_from_to_single_row_with_field()
{
    cdb_cube *cube = cdb_cube_create();
    defer { cdb_cube_destroy(cube); }

    insert_row_t *row = insert_row_create("part1001", 3);
    defer { insert_row_destroy(row); }

    insert_row_add_column_value(row, "test name", "test value") ;

    cdb_cube_insert_row(cube, row);

    {
        counter_t *count = cdb_cube_count_from_to(cube,"part0000","part1000", NULL, NULL);
        defer { free(count);  }
        mu_assert("Counted a row not from the specified interval", 0 == *count);
    }

    {
        counter_t *count = cdb_cube_count_from_to(cube,"part0000","part1001", NULL, NULL);
        defer { free(count);  }
        mu_assert("Couldn't find the row", 3 == *count);
    }

    cdb_cube_insert_row(cube, row);

    {
        counter_t *count = cdb_cube_count_from_to(cube,"part0000","part1001", NULL, NULL);
        defer { free(count);  }
        mu_assert("Failed to increment a counter", 6 == *count);

    }
    return 0;
}

static char *test_count_from_to_filter_empty()
{
    cdb_cube *cube = cdb_cube_create();
    defer { cdb_cube_destroy(cube); }

    cdb_filter *frow = cdb_filter_create();
    defer { cdb_filter_destroy(frow); }

    insert_row_t *irow = insert_row_create("part1000", 3);
    defer { insert_row_destroy(irow); }

    insert_row_add_column_value(irow, "test name", "test value");

    {
        counter_t *count = cdb_cube_count_from_to(cube, "part0000", "part1001", frow, NULL);
        defer { free(count);  }
        mu_assert("Empty cube with an empty filter", 0 == *count);
    }

    cdb_cube_insert_row(cube, irow);
    {
        counter_t *count = cdb_cube_count_from_to(cube, "part0000", "part1001", frow, NULL);
        defer { free(count);  }
        mu_assert("Couldn't find the row", 3 == *count);
    }

    cdb_cube_insert_row(cube, irow);
    {
        counter_t *count = cdb_cube_count_from_to(cube, "part0000", "part1001", frow, NULL);
        defer { free(count);  }
        mu_assert("Failed to increment a counter", 6 == *count);
    }

    return 0;
}

static char *test_count_from_to_filter_with_field()
{
    cdb_cube *cube = cdb_cube_create();
    defer { cdb_cube_destroy(cube); }

    cdb_filter *correct_frow = cdb_filter_create();
    cdb_filter_add_column_value(correct_frow, "column name", "test value");
    defer { cdb_filter_destroy(correct_frow); }

    cdb_filter *wrong_value_frow = cdb_filter_create();
    cdb_filter_add_column_value(wrong_value_frow, "column name", "other test value");
    defer { cdb_filter_destroy(wrong_value_frow); }

    cdb_filter *wrong_column_frow = cdb_filter_create();
    cdb_filter_add_column_value(wrong_column_frow, "other column name", "test value");
    defer { cdb_filter_destroy(wrong_column_frow); }

    insert_row_t *irow = insert_row_create("part1000", 3);
    insert_row_add_column_value(irow, "column name", "test value") ;
    defer { insert_row_destroy(irow); }

    {
        counter_t *count = cdb_cube_count_from_to(cube, "part0000", "part1001", correct_frow, NULL);
        defer { free(count); }
        mu_assert("Empty cube with an empty filter", 0 == *count);
    }

    {
        counter_t *count = cdb_cube_count_from_to(cube, "part0000", "part1001", wrong_value_frow, NULL);
        defer { free(count); }
        mu_assert("Empty cube with an empty filter", 0 == *count);
    }

    {
        counter_t *count = cdb_cube_count_from_to(cube, "part0000", "part1001", wrong_column_frow, NULL);
        defer { free(count); }
        mu_assert("Empty cube with an empty filter", 0 == *count);
    }

    cdb_cube_insert_row(cube, irow);

    {
        counter_t *count = cdb_cube_count_from_to(cube, "part0000", "part1001", correct_frow, NULL);
        defer { free(count); }
        mu_assert("Couldn't find the row", 3 == *count);
    }

    {
        counter_t *count = cdb_cube_count_from_to(cube, "part0000", "part1001", wrong_value_frow, NULL);
        defer { free(count); }
        mu_assert("The value should not match", 0 == *count);
    }

    {
        counter_t *count = cdb_cube_count_from_to(cube, "part0000", "part1001", wrong_column_frow, NULL);
        defer { free(count); }
        mu_assert("The column should not match", 0 == *count);
    }

    cdb_cube_insert_row(cube, irow);

    {
        counter_t *count = cdb_cube_count_from_to(cube, "part0000", "part1001", correct_frow, NULL);
        defer { free(count); }
        mu_assert("Failed to increment a counter", 6 == *count);
    }

    {
        counter_t *count = cdb_cube_count_from_to(cube, "part0000", "part1001", wrong_value_frow, NULL);
        defer { free(count); }
        mu_assert("The value should not match", 0 == *count);
    }

    {
        counter_t *count = cdb_cube_count_from_to(cube, "part0000", "part1001", wrong_column_frow, NULL);
        defer { free(count); }
        mu_assert("The column should not match", 0 == *count);
    }

    return 0;
}

static char *test_count_from_to_filter_grouped()
{
    cdb_cube *cube = cdb_cube_create();
    defer { cdb_cube_destroy(cube); }

    {
        insert_row_t *irow1 = insert_row_create("part1000", 1);
        defer { insert_row_destroy(irow1); }
        insert_row_add_column_value(irow1, "column", "test value1") ;
        cdb_cube_insert_row(cube, irow1);
    }

    {
        insert_row_t *irow2 = insert_row_create("part1001", 2);
        defer { insert_row_destroy(irow2); }
        insert_row_add_column_value(irow2, "column", "test value1") ;
        cdb_cube_insert_row(cube, irow2);
    }

    {
        insert_row_t *irow3 = insert_row_create("part1001", 4);
        defer { insert_row_destroy(irow3); }
        insert_row_add_column_value(irow3, "column", "test value2") ;
        cdb_cube_insert_row(cube, irow3);
    }

    htable_t *value_to_count = cdb_cube_count_from_to(cube, "part0000", "part1001", NULL, "column");
    defer { htable_destroy(value_to_count); }

    mu_assert("wrong value number", 2 == htable_size(value_to_count));

    counter_t *count = htable_get(value_to_count, "test value1");
    mu_assert("wrong value count", 3 == *count);

    count = htable_get(value_to_count, "test value2");
    mu_assert("wrong value count", 4 == *count);

    return 0;
}


static char *test_pcount_from_to_single_row()
{
    cdb_cube *cube = cdb_cube_create();
    defer { cdb_cube_destroy(cube); }

    {
        insert_row_t *row = insert_row_create("part1001", 3);
        cdb_cube_insert_row(cube, row);
        defer { insert_row_destroy(row); }
    }

    htable_t *partition_to_count = cdb_cube_pcount_from_to(cube,"part1001","part1002", NULL, NULL);
    defer { htable_destroy(partition_to_count); }
    mu_assert("Wrong partition num", 1 == htable_size(partition_to_count));

    counter_t *count = htable_get(partition_to_count, "part1001");
    mu_assert("Partition not found", NULL != count);
    mu_assert("Wrong partition count", 3 == *count);

    return 0;
}

static char *test_pcount_from_to_multiple_rows()
{
    cdb_cube *cube = cdb_cube_create();
    defer { cdb_cube_destroy(cube); }

    {
        insert_row_t *row1 = insert_row_create("part1001", 3);
        cdb_cube_insert_row(cube, row1);
        defer { insert_row_destroy(row1); }
    }

    {
        insert_row_t *row2 = insert_row_create("part1002", 2);
        cdb_cube_insert_row(cube, row2);
        defer { insert_row_destroy(row2); }
    }

    htable_t *partition_to_count = cdb_cube_pcount_from_to(cube,"part1001","part1002", NULL, NULL);
    defer { htable_destroy(partition_to_count); }
    mu_assert("Wrong partition num", 2 == htable_size(partition_to_count));

    {
        counter_t *count = htable_get(partition_to_count, "part1001");
        mu_assert("Partition not found", NULL != count);
        mu_assert("Wrong partition count", 3 == *count);
    }

    {
        counter_t *count = htable_get(partition_to_count, "part1002");
        mu_assert("Partition not found", NULL != count);
        mu_assert("Wrong partition count", 2 == *count);
    }

    return 0;
}

static char *test_pcount_from_to_filter_single_row()
{
    cdb_cube *cube = cdb_cube_create();
    defer { cdb_cube_destroy(cube); }

    {
        insert_row_t *irow = insert_row_create("part1001", 3);
        insert_row_add_column_value(irow, "column", "test value") ;
        defer { insert_row_destroy(irow); }
        cdb_cube_insert_row(cube, irow);
    }

    {
        cdb_filter *correct_frow = cdb_filter_create();
        cdb_filter_add_column_value(correct_frow, "column", "test value");
        defer { cdb_filter_destroy(correct_frow); }

        htable_t *correct_partition_to_count = cdb_cube_pcount_from_to(cube,"part1001","part1002", correct_frow, NULL);
        defer { htable_destroy(correct_partition_to_count); }
        mu_assert("Wrong partition num", 1 == htable_size(correct_partition_to_count));

        counter_t *count = htable_get(correct_partition_to_count, "part1001");
        mu_assert("Partition not found", NULL != count);
        mu_assert("Wrong partition count", 3 == *count);
    }

    {
        cdb_filter *wrong_value_frow = cdb_filter_create();
        cdb_filter_add_column_value(wrong_value_frow, "column", "wrong value");
        defer { cdb_filter_destroy(wrong_value_frow); }

        htable_t *wrong_partition_to_count = cdb_cube_pcount_from_to(cube,"part1001","part1002", wrong_value_frow, NULL);
        defer { htable_destroy(wrong_partition_to_count); }
        mu_assert("Wrong partition num", 1 == htable_size(wrong_partition_to_count));

        counter_t *count = htable_get(wrong_partition_to_count, "part1001");
        mu_assert("Partition found", NULL != count);
        mu_assert("Wrong partition count", 0 == *count);
    }

    return 0;
}

static char *test_pcount_from_to_grouped()
{
    cdb_cube *cube = cdb_cube_create();
    defer { cdb_cube_destroy(cube); }

    {
        insert_row_t *irow1 = insert_row_create("part1001", 3);
        insert_row_add_column_value(irow1, "column", "test value") ;
        defer { insert_row_destroy(irow1); }
        cdb_cube_insert_row(cube, irow1);

    }

    {
        insert_row_t *irow2 = insert_row_create("part1001", 2);
        insert_row_add_column_value(irow2, "column", "test value2") ;
        defer { insert_row_destroy(irow2); }
        cdb_cube_insert_row(cube, irow2);
    }

    {
        insert_row_t *irow3 = insert_row_create("part1001", 1);
        insert_row_add_column_value(irow3, "column", "test value3") ;
        defer { insert_row_destroy(irow3); }
        cdb_cube_insert_row(cube, irow3);
    }

    htable_t *partition_to_value_to_count = cdb_cube_pcount_from_to(cube, "part1001", "part1001", NULL, "column");
    htable_t *value_to_count = htable_get(partition_to_value_to_count, "part1001");
    defer { htable_destroy(partition_to_value_to_count); }

    {
        counter_t *count = htable_get(value_to_count, "test value");
        mu_assert("wrong value count", 3 == *count);
    }

    {
        counter_t *count = htable_get(value_to_count, "test value2");
        mu_assert("wrong value count", 2 == *count);
    }

    {
        counter_t *count = htable_get(value_to_count, "test value3");
        mu_assert("wrong value count", 1 == *count);
    }

    return 0;
}

static char *test_pcount_from_to_filter_grouped()
{
    cdb_cube *cube = cdb_cube_create();
    defer { cdb_cube_destroy(cube); }

    {
        insert_row_t *irow1 = insert_row_create("part1001", 3);
        insert_row_add_column_value(irow1, "column", "test value") ;
        defer { insert_row_destroy(irow1); }

        insert_row_t *irow2 = insert_row_create("part1001", 2);
        insert_row_add_column_value(irow2, "column", "test value2") ;
        defer { insert_row_destroy(irow2); }

        insert_row_t *irow3 = insert_row_create("part1001", 1);
        insert_row_add_column_value(irow3, "column", "test value3") ;
        defer { insert_row_destroy(irow3); }

        cdb_cube_insert_row(cube, irow1);
        cdb_cube_insert_row(cube, irow2);
        cdb_cube_insert_row(cube, irow3);
    }

    cdb_filter *frow = cdb_filter_create();
    cdb_filter_add_column_value(frow, "column", "test value2");
    cdb_filter_add_column_value(frow, "column", "test value3");
    defer { cdb_filter_destroy(frow); }

    htable_t *partition_to_value_to_count = cdb_cube_pcount_from_to(cube, "part1001", "part1001", frow, "column");
    defer { htable_destroy(partition_to_value_to_count); }

    htable_t *value_to_count = htable_get(partition_to_value_to_count, "part1001");

    mu_assert("wrong value number", 2 == htable_size(value_to_count));

    {
        counter_t *count = htable_get(value_to_count, "test value2");
        mu_assert("wrong value count", 2 == *count);
    }

    {
        counter_t *count = htable_get(value_to_count, "test value3");
        mu_assert("wrong value count", 1 == *count);
    }

    return 0;
}

static char *test_get_columns_to_value_set()
{
    cdb_cube *cube = cdb_cube_create();
    defer { cdb_cube_destroy(cube); }

    /* Nothing in the cube yet */

    {
        htable_t *columns_to_value_set = cdb_cube_get_columns_to_value_set(cube, NULL, NULL);
        defer { htable_destroy(columns_to_value_set); }
        mu_assert("no columns should be there yet", 0 == htable_size(columns_to_value_set));
    }

    /* Insert a few row in different partitions */
    {
        insert_row_t *irow = insert_row_create("part1001", 3);
        insert_row_add_column_value(irow, "column", "test value") ;
        defer { insert_row_destroy(irow); }
        cdb_cube_insert_row(cube, irow);
    }

    {
        insert_row_t *irow = insert_row_create("part1002", 2);
        insert_row_add_column_value(irow, "column", "test value") ;
        defer { insert_row_destroy(irow); }
        cdb_cube_insert_row(cube, irow);
    }

    {
        insert_row_t *irow = insert_row_create("part1003", 2);
        insert_row_add_column_value(irow, "column", "test value2") ;
        defer { insert_row_destroy(irow); }
        cdb_cube_insert_row(cube, irow);
    }

    {
        insert_row_t *irow = insert_row_create("part1003", 2);
        insert_row_add_column_value(irow, "column", "test value3") ;
        defer { insert_row_destroy(irow); }
        cdb_cube_insert_row(cube, irow);
    }

    {
        insert_row_t *irow = insert_row_create("part1003", 2);
        insert_row_add_column_value(irow, "column2", "test value3") ;
        defer { insert_row_destroy(irow); }
        cdb_cube_insert_row(cube, irow);
    }

    /* Check without a partition filter */

    {
        htable_t *columns_to_value_set = cdb_cube_get_columns_to_value_set(cube, NULL, NULL);
        defer { htable_destroy(columns_to_value_set); }
        mu_assert("wrong column number", 2 == htable_size(columns_to_value_set));
        mu_assert("column not present", htable_has(columns_to_value_set, "column"));
        mu_assert("column not present", htable_has(columns_to_value_set, "column2"));

        htable_t *value_set = htable_get(columns_to_value_set, "column");
        mu_assert("wrong value number", 3 == htable_size(value_set));
        mu_assert("value not present", htable_has(value_set, "test value"));
        mu_assert("value not present", htable_has(value_set, "test value2"));
        mu_assert("value not present", htable_has(value_set, "test value3"));

        value_set = htable_get(columns_to_value_set, "column2");
        mu_assert("wrong value number", 1 == htable_size(value_set));
        mu_assert("value not present", htable_has(value_set, "test value3"));
    }

    /* Check with a partition filter */

    {
        htable_t *columns_to_value_set = cdb_cube_get_columns_to_value_set(cube, "part1001", "part1002");
        defer { htable_destroy(columns_to_value_set); }
        mu_assert("wrong column number", 1 == htable_size(columns_to_value_set));
        mu_assert("column not present", htable_has(columns_to_value_set, "column"));

        htable_t *value_set = htable_get(columns_to_value_set, "column");
        mu_assert("wrong value number", 1 == htable_size(value_set));
        mu_assert("value not present", htable_has(value_set, "test value"));
    }

    return 0;
}

static char *test_max_value()
{
    cdb_cube *cube = cdb_cube_create();
    defer { cdb_cube_destroy(cube); }

    /* Insert A LOT of values, but below the limit */
    for (size_t i = 0; i <= VALUE_ID_MAX; ++i) {
        sds msg = sdscatprintf(sdsempty(), "%zu", i);
        defer { sdsfree(msg); }

        insert_row_t *irow = insert_row_create("part", 1);
        insert_row_add_column_value(irow, "column", msg) ;
        defer { insert_row_destroy(irow); }
        mu_assert("failed to insert a row", cdb_cube_insert_row(cube, irow));
    }

    /* The last insertion should fail */
    {
        insert_row_t *irow = insert_row_create("part", 1);
        insert_row_add_column_value(irow, "column", "one last value") ;
        defer { insert_row_destroy(irow); }
        mu_assert("should not be able to insert a row", !cdb_cube_insert_row(cube, irow));
    }

    return 0;
}

static char *all_tests()
{
    mu_run_test(test_empty);

    mu_run_test(test_multiple_inserts);

    mu_run_test(test_count_from_to_single_row);
    mu_run_test(test_count_from_to_single_row_with_field);
    mu_run_test(test_count_from_to_filter_empty);
    mu_run_test(test_count_from_to_filter_with_field);
    mu_run_test(test_count_from_to_filter_grouped);

    mu_run_test(test_pcount_from_to_single_row);
    mu_run_test(test_pcount_from_to_multiple_rows);
    mu_run_test(test_pcount_from_to_filter_single_row);
    mu_run_test(test_pcount_from_to_grouped);
    mu_run_test(test_pcount_from_to_filter_grouped);

    mu_run_test(test_get_columns_to_value_set);
    mu_run_test(test_max_value);
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
