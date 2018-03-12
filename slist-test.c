#include <stdio.h>
#include <assert.h>
#include <stdbool.h>

#include "minunit.h"
#include "slist.h"
#include "defs.h"

int tests_run = 0;

char *test_slist_create()
{
    slist_t *list = slist_create();
    defer { slist_destroy(list); }

    mu_assert("List is NULL", list);
    mu_assert("List's head is not null", NULL == list->head);

    return 0;
}

char *test_slist_prepend()
{
    slist_t *list = slist_create();
    defer { slist_destroy(list); }

    int i1 = 0;
    slist_prepend(list, &i1);
    int *data = list->head->data;
    mu_assert("Couldn't find the prepended element", *data == i1);

    int i2 = 1;
    slist_prepend(list, &i2);
    data = list->head->data;
    mu_assert("Couldn't find the second prepended element", *data == i2);

    return 0;
}

char *test_slist_for_each()
{
    slist_t *list = slist_create();
    defer { slist_destroy(list); }

    int i1 = 1, i2 = 2;
    slist_prepend(list, &i1);
    slist_prepend(list, &i2);

    int sum = 0;
    slist_for_each(node, list) {
        int *elem = slist_data(node);
        sum += *elem;
    }

    mu_assert("The sum of the elements is wrong", sum == i1 + i2);
    return 0;
}

char *test_slist_for_each_filter()
{
    slist_t *list = slist_create();
    defer { slist_destroy(list); }

    int i1 = 1, i2 = 2, i3 = 3, i4 = 4;
    slist_prepend(list, &i1);
    slist_prepend(list, &i2);
    slist_prepend(list, &i3);
    slist_prepend(list, &i4);

    bool filter(void *data) {
        int *element = data;
        return *element > i1 && *element < i4;
    }

    int sum = 0;
    slist_for_each_filter(node, list, filter) {
        int *elem = slist_data(node);
        sum += *elem;
    }

    mu_assert("The sum of the elements is wrong", sum == i2 + i3);
    return 0;
}

char *test_slist_find()
{
    slist_t *list = slist_create();
    defer { slist_destroy(list); }

    bool finder(void *data) {
        int *element = data;
        return 2 == *element;
    }

    int *result = slist_find(list, finder);
    mu_assert("found result in an empty list", NULL == result);

    int i1 = 1, i2 = 2;
    slist_prepend(list, &i1);
    slist_prepend(list, &i2);

    result = slist_find(list, finder);
    mu_assert("find couldn't find the result in a list", NULL != result);
    mu_assert("find result is not correct in a list", 2 == *result);

    return 0;
}

char *test_slist_delete_single_if()
{
    slist_t *list = slist_create();
    defer { slist_destroy(list); }

    bool finder(void *data) {
        int *element = data;
        return 2 == *element;
    }

    int *result = slist_find(list, finder);
    mu_assert("found result in an empty list", NULL == result);

    int i1 = 1, i2 = 2;
    slist_prepend(list, &i1);
    slist_prepend(list, &i2);

    int *res = slist_delete_single_if(list, finder);
    mu_assert("deleted a wrong element", 2 == *res);
    mu_assert("wrong list size", 1 == slist_size(list));

    res = slist_find(list, finder);
    mu_assert("found a deleted element", NULL == res);

    int count = 0;
    slist_for_each(node, list) {
        count++;
    }
    mu_assert("wrong element number", count == 1);

    return 0;
}


static char *all_tests()
{
    mu_run_test(test_slist_create);
    mu_run_test(test_slist_prepend);
    mu_run_test(test_slist_for_each);
    mu_run_test(test_slist_for_each_filter);
    mu_run_test(test_slist_find);
    mu_run_test(test_slist_delete_single_if);
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
