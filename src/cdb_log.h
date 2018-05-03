#ifndef CDB_LOG_H
#define CDB_LOG_H

#include <stdarg.h>

#define LOG_DEBUG 3
#define LOG_VERBOSE 2
#define LOG_INFO 1
#define LOG_WARNING 0

#define log_debug(...) cdb_log(LOG_DEBUG,  __VA_ARGS__)
#define log_verb(...) cdb_log(LOG_VERBOSE, __VA_ARGS__)
#define log_info(...) cdb_log(LOG_INFO, __VA_ARGS__)
#define log_warn(...) cdb_log(LOG_WARNING, __VA_ARGS__)

void cdb_log(int level, const char *fmt, ...);

#endif //CDB_LOG_H
