#ifndef MINUNIT_H
#define MINUNIT_H

/* NOTE: Only need this here because some of the tested files depend on extern config */
#include "config.h"
config_t *config = NULL;

#define mu_to_string_helper(linum) #linum
#define mu_to_string(linum) mu_to_string_helper(linum)
#define mu_assert(message, test) do { if (!(test)) return __FILE__ ":" mu_to_string(__LINE__) " " message; } while (0)
#define mu_run_test(test) do { char *message = test(); tests_run++; \
        if (message) return message; } while (0)
extern int tests_run;

#endif //MINUNIT_H
