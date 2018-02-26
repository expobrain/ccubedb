#include <stdio.h>
#include <stdlib.h>

#include "minunit.h"
#include "cubedb.h"
#include "sds.h"

int tests_run = 0;

char *test_find_cube()
{
    cubedb_t *cubedb = cubedb_create();
    defer { cubedb_destroy(cubedb); }

    cube_t *test_cube = cubedb_find_cube(cubedb,"test cube");
    mu_assert("Found a cube that should not exist", test_cube == NULL);

    cube_t *cube = cube_create();

    cubedb_add_cube(cubedb,"test cube", cube);
    test_cube = cubedb_find_cube(cubedb,"test cube");
    mu_assert("Failed to find a cube", test_cube != NULL);

    test_cube = cubedb_find_cube(cubedb, "test_cube2");
    mu_assert("Found a non-existant cube", test_cube == NULL);

    return 0;
}

static char *all_tests()
{
    mu_run_test(test_find_cube);
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
