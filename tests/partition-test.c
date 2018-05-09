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

static char *all_tests()
{
    mu_run_test(test_empty);
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
