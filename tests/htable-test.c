#include <stdio.h>

#include "minunit.h"
#include "htable.h"
#include "cdb_defs.h"

int tests_run = 0;

char *test_htable_create()
{
    /* Just make sure nothing crashes and valgrind doesn't complain here */
    htable_t *table = htable_create(1024, NULL);
    defer { htable_destroy(table); }

    mu_assert("stupid sanity check", 0 == htable_size(table));
    return 0;
}

char *test_htable_put_get()
{
    htable_t *table = htable_create(1024, NULL);
    defer { htable_destroy(table); }

    mu_assert("There should be nothing in the table", NULL == htable_get(table, "random key"));

    int value = 100;
    htable_put(table, "proper key", &value);

    mu_assert("Found a non-existing key", NULL == htable_get(table,"non-existing key"));

    int *value_found = htable_get(table,"proper key");
    mu_assert("The key should be in the table", value_found);
    mu_assert("Wrong value in the table", value == *value_found);

    return 0;
}

char *test_htable_put_put()
{
    htable_t *table = htable_create(1024, NULL);
    defer { htable_destroy(table); }

    int value1 = 100;
    int value2 = 101;
    htable_put(table, "key", &value1);
    htable_put(table, "key", &value2);

    int *value_found = htable_get(table,"key");
    mu_assert("Wrong value in the table", value2 == *value_found);

    return 0;
}

char *test_htable_put_del()
{
    htable_t *table = htable_create(1024, NULL);
    defer { htable_destroy(table); }

    mu_assert("There should be nothing in the table", NULL == htable_get(table, "proper key"));

    int value = 100;
    htable_put(table, "proper key", &value);

    mu_assert("Found a non-existing key", NULL == htable_get(table,"non-existing key"));

    int *value_found = htable_get(table,"proper key");
    mu_assert("The table should not contain anything", 1 == htable_size(table));
    mu_assert("The key should be in the table", value_found);
    mu_assert("Wrong value in the table", value == *value_found);

    htable_del(table, "proper key");
    value_found = htable_get(table,"proper key");
    mu_assert("The key should not be there", !value_found);
    mu_assert("The table should not contain anything", 0 == htable_size(table));

    return 0;
}


char *test_htable_for_each()
{
    htable_t *table = htable_create(1024, NULL);
    defer { htable_destroy(table); }

    int value0 = 100;
    int value1 = 101;
    int value2 = 102;
    htable_put(table, "key0", &value0);
    htable_put(table, "key1", &value1);
    htable_put(table, "key2", &value2);

    int value_count = 0;
    int value_sum = 0;
    htable_for_each(item, table) {
        value_count++;
        int *value = htable_value(item);
        value_sum += *value;
    }
    mu_assert("Wrong number of elements in htable", 3 == value_count);
    mu_assert("Wrong sum of elements in htable", value0 + value1 + value2 == value_sum);

    return 0;
}

char *test_htable_for_each_filter()
{
    htable_t *table = htable_create(1024, NULL);
    defer { htable_destroy(table); }

    int value0 = 100;
    int value1 = 101;
    int value2 = 102;
    htable_put(table, "key0", &value0);
    htable_put(table, "key1", &value1);
    htable_put(table, "key2", &value2);

    bool filter(void *htkey) {
        char *key = htkey;
        return 0 == strcmp(key,"key1");
    }

    int value_count = 0;
    int value_sum = 0;
    htable_for_each_filter(item, table, filter) {
        value_count++;
        int *value = htable_value(item);
        value_sum += *value;
    }
    mu_assert("Wrong number of elements in htable after filtering", 1 == value_count);
    mu_assert("Wrong sum of elements in htable after filtering", value1 == value_sum);

    return 0;
}


char *test_htable_resize()
{
    htable_t *table = htable_create(2, NULL);
    defer { htable_destroy(table); }

    int value0 = 100;
    int value1 = 101;
    int value2 = 102;
    int value3 = 103;

    mu_assert("Wrong table size", 2 == htable_table_size(table));
    htable_put(table, "key0", &value0);
    mu_assert("Wrong table size", 2 == htable_table_size(table));
    htable_put(table, "key1", &value1);
    mu_assert("Wrong table size", 4 == htable_table_size(table));
    htable_put(table, "key2", &value2);
    mu_assert("Wrong table size", 8 == htable_table_size(table));
    htable_put(table, "key3", &value3);
    mu_assert("Wrong table size", 8 == htable_table_size(table));

    int value_count = 0;
    int value_sum = 0;
    htable_for_each(item, table) {
        value_count++;
        int *value = htable_value(item);
        value_sum += *value;
    }
    mu_assert("Wrong number of elements in htable", 4 == value_count);
    mu_assert("Wrong sum of elements in htable", value0 + value1 + value2 + value3 == value_sum);
    return 0;
}

static char *all_tests()
{
    mu_run_test(test_htable_create);
    mu_run_test(test_htable_put_get);
    mu_run_test(test_htable_put_put);
    mu_run_test(test_htable_put_del);
    mu_run_test(test_htable_for_each);
    mu_run_test(test_htable_for_each_filter);
    mu_run_test(test_htable_resize);
    return 0;
}

int main(int argc, char **argv)
{
    (void) argc; (void) argv;
    char *result = all_tests();
    if (result != 0)
        printf("%s\n", result);
    else
        printf("ALL TESTS PASSED: %s\n", __FILE__);
    printf("Tests run: %d\n", tests_run);

    return result != 0;
}
