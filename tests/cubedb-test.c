#include <stdio.h>
#include <stdlib.h>

#include "minunit.h"
#include "cdb_cubedb.h"
#include "sds.h"

int tests_run = 0;

char *test_find_cube()
{
    cdb_cubedb *cubedb = cdb_cubedb_create();
    defer { cdb_cubedb_destroy(cubedb); }

    cdb_cube *test_cube = cdb_cubedb_find_cube(cubedb,"test cube");
    mu_assert("Found a cube that should not exist", test_cube == NULL);

    cdb_cube *cube = cdb_cube_create();

    cdb_cubedb_add_cube(cubedb,"test cube", cube);
    test_cube = cdb_cubedb_find_cube(cubedb,"test cube");
    mu_assert("Failed to find a cube", test_cube != NULL);

    test_cube = cdb_cubedb_find_cube(cubedb, "test_cube2");
    mu_assert("Found a non-existant cube", test_cube == NULL);

    return 0;
}

char *test_add_delete_cube()
{
    cdb_cubedb *cubedb = cdb_cubedb_create();

    {
        cdb_cube *test_cube = cdb_cubedb_find_cube(cubedb,"cube1");
        mu_assert("Found a cube that should not exist", test_cube == NULL);
    }

    {
        cdb_cube *test_cube = cdb_cubedb_find_cube(cubedb,"cube2");
        mu_assert("Found a cube that should not exist", test_cube == NULL);
    }

    {
        cdb_cube *test_cube = cdb_cubedb_find_cube(cubedb,"cube3");
        mu_assert("Found a cube that should not exist", test_cube == NULL);
    }

    {
        cdb_cube *cube = cdb_cube_create();
        cdb_cubedb_add_cube(cubedb,"cube1",cube);
        cdb_cube *test_cube = cdb_cubedb_find_cube(cubedb,"cube1");
        mu_assert("Couldn't find a cube", cube == test_cube);
    }

    {
        cdb_cube *cube = cdb_cube_create();
        cdb_cubedb_add_cube(cubedb,"cube2",cube);
        cdb_cube *test_cube = cdb_cubedb_find_cube(cubedb,"cube2");
        mu_assert("Couldn't find a cube", cube == test_cube);
    }

    {
        cdb_cube *cube = cdb_cube_create();
        cdb_cubedb_add_cube(cubedb,"cube3",cube);
        cdb_cube *test_cube = cdb_cubedb_find_cube(cubedb,"cube3");
        mu_assert("Couldn't find a cube", cube == test_cube);
    }

    cdb_cubedb_del_cube(cubedb,"cube1");
    cdb_cubedb_del_cube(cubedb,"cube2");
    /* NOTE: Skipping cube3 as it should be destroy along with the db */

    cdb_cubedb_destroy(cubedb);

    return 0;
}

char *test_for_each_simple()
{
    cdb_cubedb *cubedb = cdb_cubedb_create();
    defer { cdb_cubedb_destroy(cubedb); }

    cdb_cube *cube1 = cdb_cube_create();
    cdb_cubedb_add_cube(cubedb,"test cube1", cube1);

    {
        cdb_insert_row *irow = cdb_insert_row_create("part1", 1);
        defer { cdb_insert_row_destroy(irow); }
        cdb_insert_row_add_column_value(irow, "column1", "val1") ;
        cdb_cube_insert_row(cube1, irow);
    }

    {
        cdb_insert_row *irow = cdb_insert_row_create("part2", 1);
        defer { cdb_insert_row_destroy(irow); }
        cdb_insert_row_add_column_value(irow, "column1", "val1") ;
        cdb_cube_insert_row(cube1, irow);

    }

    cdb_cube *cube2 = cdb_cube_create();
    cdb_cubedb_add_cube(cubedb,"test cube2", cube2);

    {
        cdb_insert_row *irow = cdb_insert_row_create("part3", 1);
        defer { cdb_insert_row_destroy(irow); }
        cdb_insert_row_add_column_value(irow, "column1", "val1") ;
        cdb_cube_insert_row(cube2, irow);
    }

    {
        cdb_insert_row *irow = cdb_insert_row_create("part4", 1);
        defer { cdb_insert_row_destroy(irow); }
        cdb_insert_row_add_column_value(irow, "column1", "val1") ;
        cdb_cube_insert_row(cube2, irow);

    }

    uint64_t partition_count = 0;
    bool cube1_part1 = false;
    bool cube1_part2 = false;
    bool cube2_part3 = false;
    bool cube2_part4 = false;

    void cube_visitor(sds cube_name, sds partition_name, cdb_partition * partition)
    {
        (void) partition;
        partition_count++;
        if (0 == strcmp(cube_name, "cube1") && 0 == strcmp(partition_name, "part1"))
            cube1_part1 = true;
        if (0 == strcmp(cube_name, "cube1") && 0 == strcmp(partition_name, "part2"))
            cube1_part2 = true;
        if (0 == strcmp(cube_name, "cube2") && 0 == strcmp(partition_name, "part3"))
            cube2_part3 = true;
        if (0 == strcmp(cube_name, "cube2") && 0 == strcmp(partition_name, "part4"))
            cube2_part4 = true;
    }

    cdb_cubedb_for_each_cube_partition(cubedb, cube_visitor);

    mu_assert("Wrong partition count", 4 == partition_count);
    mu_assert("Cube partitions not found", !cube1_part1 || !cube1_part2 || !cube2_part3 || !cube2_part4);

    return 0;
}


static char *all_tests()
{
    mu_run_test(test_find_cube);
    mu_run_test(test_add_delete_cube);
    mu_run_test(test_for_each_simple);
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
