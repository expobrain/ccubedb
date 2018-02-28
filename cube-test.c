#include <stdio.h>
#include <stdlib.h>

#include "minunit.h"
#include "cube.h"
#include "partition.h"
#include "defs.h"
#include "htable.h"

int tests_run = 0;

static char *test_empty()
{
    cube_t *cube = cube_create();
    defer { cube_destroy(cube); }

    counter_t *count = cube_count_from_to(cube,"part0","part9999", NULL, NULL);
    defer { free(count); }

    mu_assert("There is nothing in the cube and count != 0", 0 == *count);
    return 0;
}

static char *test_count_from_to_single_row()
{
    cube_t *cube = cube_create();
    defer { cube_destroy(cube); }

    insert_row_t *row = insert_row_create("part1001", 3);
    cube_insert_row(cube, row);
    defer { insert_row_destroy(row); }

    {
        counter_t *count = cube_count_from_to(cube,"part0000","part1000", NULL, NULL);
        defer { free(count); count = NULL; }
        mu_assert("Counted a row not from the specified interval", 0 == *count);
    }

    {
        counter_t *count = cube_count_from_to(cube,"part0000","part1001", NULL, NULL);
        defer { free(count); count = NULL; }
        mu_assert("Couldn't find the row!", 3 == *count);
    }

    return 0;
}

static char *test_count_from_to_single_row_with_field()
{
    cube_t *cube = cube_create();
    defer { cube_destroy(cube); }

    insert_row_t *row = insert_row_create("part1001", 3);
    defer { insert_row_destroy(row); }

    insert_row_add_column_value(row, "test name", "test value") ;

    cube_insert_row(cube, row);

    {
        counter_t *count = cube_count_from_to(cube,"part0000","part1000", NULL, NULL);
        defer { free(count); count = NULL; }
        mu_assert("Counted a row not from the specified interval", 0 == *count);
    }

    {
        counter_t *count = cube_count_from_to(cube,"part0000","part1001", NULL, NULL);
        defer { free(count); count = NULL; }
        mu_assert("Couldn't find the row", 3 == *count);
    }

    cube_insert_row(cube, row);

    {
        counter_t *count = cube_count_from_to(cube,"part0000","part1001", NULL, NULL);
        defer { free(count); count = NULL; }
        mu_assert("Failed to increment a counter", 6 == *count);

    }
    return 0;
}

static char *test_count_from_to_filter_empty()
{
    cube_t *cube = cube_create();
    defer { cube_destroy(cube); }

    filter_t *frow = filter_create();
    defer { filter_destroy(frow); }

    insert_row_t *irow = insert_row_create("part1000", 3);
    defer { insert_row_destroy(irow); }

    insert_row_add_column_value(irow, "test name", "test value") ;

    counter_t *count = NULL;
    {
        count = cube_count_from_to(cube, "part0000", "part1001", frow, NULL);
        defer { free(count); count = NULL; }
        mu_assert("Empty cube with an empty filter", 0 == *count);
    }

    cube_insert_row(cube, irow);
    {
        count = cube_count_from_to(cube, "part0000", "part1001", frow, NULL);
        defer { free(count); count = NULL; }
        mu_assert("Couldn't find the row", 3 == *count);
    }

    cube_insert_row(cube, irow);
    {
        count = cube_count_from_to(cube, "part0000", "part1001", frow, NULL);
        defer { free(count); count = NULL; }
        mu_assert("Failed to increment a counter", 6 == *count);
    }

    return 0;
}

static char *test_count_from_to_filter_with_field()
{
    cube_t *cube = cube_create();
    defer { cube_destroy(cube); }

    filter_t *correct_frow = filter_create();
    filter_add_column_value(correct_frow, "column name", "test value");
    defer { filter_destroy(correct_frow); }

    filter_t *wrong_value_frow = filter_create();
    filter_add_column_value(wrong_value_frow, "column name", "other test value");
    defer { filter_destroy(wrong_value_frow); }

    filter_t *wrong_column_frow = filter_create();
    filter_add_column_value(wrong_column_frow, "other column name", "test value");
    defer { filter_destroy(wrong_column_frow); }

    insert_row_t *irow = insert_row_create("part1000", 3);
    insert_row_add_column_value(irow, "column name", "test value") ;
    defer { insert_row_destroy(irow); }

    {
        counter_t *count = cube_count_from_to(cube, "part0000", "part1001", correct_frow, NULL);
        defer { free(count); }
        mu_assert("Empty cube with an empty filter", 0 == *count);
    }

    {
        counter_t *count = cube_count_from_to(cube, "part0000", "part1001", wrong_value_frow, NULL);
        defer { free(count); }
        mu_assert("Empty cube with an empty filter", 0 == *count);
    }

    {
        counter_t *count = cube_count_from_to(cube, "part0000", "part1001", wrong_column_frow, NULL);
        defer { free(count); }
        mu_assert("Empty cube with an empty filter", 0 == *count);
    }

    cube_insert_row(cube, irow);

    {
        counter_t *count = cube_count_from_to(cube, "part0000", "part1001", correct_frow, NULL);
        defer { free(count); }
        mu_assert("Couldn't find the row", 3 == *count);
    }

    {
        counter_t *count = cube_count_from_to(cube, "part0000", "part1001", wrong_value_frow, NULL);
        defer { free(count); }
        mu_assert("The value should not match", 0 == *count);
    }

    {
        counter_t *count = cube_count_from_to(cube, "part0000", "part1001", wrong_column_frow, NULL);
        defer { free(count); }
        mu_assert("The column should not match", 0 == *count);
    }

    cube_insert_row(cube, irow);

    {
        counter_t *count = cube_count_from_to(cube, "part0000", "part1001", correct_frow, NULL);
        defer { free(count); }
        mu_assert("Failed to increment a counter", 6 == *count);
    }

    {
        counter_t *count = cube_count_from_to(cube, "part0000", "part1001", wrong_value_frow, NULL);
        defer { free(count); }
        mu_assert("The value should not match", 0 == *count);
    }

    {
        counter_t *count = cube_count_from_to(cube, "part0000", "part1001", wrong_column_frow, NULL);
        defer { free(count); }
        mu_assert("The column should not match", 0 == *count);
    }

    return 0;
}

static char *test_count_from_to_filter_grouped()
{
    cube_t *cube = cube_create();
    defer { cube_destroy(cube); }

    {
        insert_row_t *irow1 = insert_row_create("part1000", 1);
        defer { insert_row_destroy(irow1); }
        insert_row_add_column_value(irow1, "column", "test value1") ;
        cube_insert_row(cube, irow1);
    }

    {
        insert_row_t *irow2 = insert_row_create("part1001", 2);
        defer { insert_row_destroy(irow2); }
        insert_row_add_column_value(irow2, "column", "test value1") ;
        cube_insert_row(cube, irow2);
    }

    {
        insert_row_t *irow3 = insert_row_create("part1001", 4);
        defer { insert_row_destroy(irow3); }
        insert_row_add_column_value(irow3, "column", "test value2") ;
        cube_insert_row(cube, irow3);
    }

    htable_t *value_to_count = cube_count_from_to(cube, "part0000", "part1001", NULL, "column");
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
    cube_t *cube = cube_create();
    defer { cube_destroy(cube); }

    {
        insert_row_t *row = insert_row_create("part1001", 3);
        cube_insert_row(cube, row);
        defer { insert_row_destroy(row); }
    }

    htable_t *partition_to_count = cube_pcount_from_to(cube,"part1001","part1002", NULL, NULL);
    defer { htable_destroy(partition_to_count); }
    mu_assert("Wrong partition num", 1 == htable_size(partition_to_count));

    counter_t *count = htable_get(partition_to_count, "part1001");
    mu_assert("Partition not found", NULL != count);
    mu_assert("Wrong partition count", 3 == *count);

    return 0;
}

static char *test_pcount_from_to_multiple_rows()
{
    cube_t *cube = cube_create();
    defer { cube_destroy(cube); }

    {
        insert_row_t *row1 = insert_row_create("part1001", 3);
        cube_insert_row(cube, row1);
        defer { insert_row_destroy(row1); }
    }

    {
        insert_row_t *row2 = insert_row_create("part1002", 2);
        cube_insert_row(cube, row2);
        defer { insert_row_destroy(row2); }
    }

    htable_t *partition_to_count = cube_pcount_from_to(cube,"part1001","part1002", NULL, NULL);
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
    cube_t *cube = cube_create();
    defer { cube_destroy(cube); }

    {
        insert_row_t *irow = insert_row_create("part1001", 3);
        insert_row_add_column_value(irow, "column", "test value") ;
        defer { insert_row_destroy(irow); }
        cube_insert_row(cube, irow);
    }

    {
        filter_t *correct_frow = filter_create();
        filter_add_column_value(correct_frow, "column", "test value");
        defer { filter_destroy(correct_frow); }

        htable_t *correct_partition_to_count = cube_pcount_from_to(cube,"part1001","part1002", correct_frow, NULL);
        defer { htable_destroy(correct_partition_to_count); }
        mu_assert("Wrong partition num", 1 == htable_size(correct_partition_to_count));

        counter_t *count = htable_get(correct_partition_to_count, "part1001");
        mu_assert("Partition not found", NULL != count);
        mu_assert("Wrong partition count", 3 == *count);
    }

    {
        filter_t *wrong_value_frow = filter_create();
        filter_add_column_value(wrong_value_frow, "column", "wrong value");
        defer { filter_destroy(wrong_value_frow); }

        htable_t *wrong_partition_to_count = cube_pcount_from_to(cube,"part1001","part1002", wrong_value_frow, NULL);
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
    cube_t *cube = cube_create();
    defer { cube_destroy(cube); }

    insert_row_t *irow1 = insert_row_create("part1001", 3);
    insert_row_add_column_value(irow1, "column", "test value") ;
    defer { insert_row_destroy(irow1); }

    insert_row_t *irow2 = insert_row_create("part1001", 2);
    insert_row_add_column_value(irow2, "column", "test value2") ;
    defer { insert_row_destroy(irow2); }

    insert_row_t *irow3 = insert_row_create("part1001", 1);
    insert_row_add_column_value(irow3, "column", "test value3") ;
    defer { insert_row_destroy(irow3); }

    cube_insert_row(cube, irow1);
    cube_insert_row(cube, irow2);
    cube_insert_row(cube, irow3);

    htable_t *partition_to_value_to_count = cube_pcount_from_to(cube, "part1001", "part1001", NULL, "column");
    htable_t *value_to_count = htable_get(partition_to_value_to_count, "part1001");
    defer { htable_destroy(partition_to_value_to_count); }

    counter_t *count = NULL;
    count = htable_get(value_to_count, "test value");
    mu_assert("wrong value count", 3 == *count);

    count = htable_get(value_to_count, "test value2");
    mu_assert("wrong value count", 2 == *count);

    count = htable_get(value_to_count, "test value3");
    mu_assert("wrong value count", 1 == *count);

    return 0;
}

static char *test_pcount_from_to_filter_grouped()
{
    cube_t *cube = cube_create();
    defer { cube_destroy(cube); }

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

        cube_insert_row(cube, irow1);
        cube_insert_row(cube, irow2);
        cube_insert_row(cube, irow3);
    }

    filter_t *frow = filter_create();
    filter_add_column_value(frow, "column", "test value2");
    filter_add_column_value(frow, "column", "test value3");
    defer { filter_destroy(frow); }

    htable_t *partition_to_value_to_count = cube_pcount_from_to(cube, "part1001", "part1001", frow, "column");
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



static char *all_tests()
{
    mu_run_test(test_empty);

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
    return 0;
}

int main(int argc, char **argv)
{
    (void) argc; (void) argv;
    char *result = all_tests();
    if (result != 0) {
        printf("%s\n", result);
    }
    else {
        printf("ALL TESTS PASSED: %s\n", __FILE__);
    }
    printf("Tests run: %d\n", tests_run);

    return result != 0;
}
