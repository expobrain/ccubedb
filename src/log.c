#include <stdarg.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <assert.h>

#include "log.h"
#include "config.h"

#define LOG_MAX_LEN 1024

extern config_t *config;

static char *level_to_string[] = {
    [LOG_DEBUG] = "DEBUG",
    [LOG_VERBOSE] = "VERBOSE",
    [LOG_INFO] = "INFO",
    [LOG_WARNING] = "WARNING",
};

static void do_log(int level, char *msg)
{
    FILE *fp = config->log_path ? fopen(config->log_path,"a") : stdout;
    if (!fp) return;

    {
        char timestamp_str[64];
        struct timeval tv;
        gettimeofday(&tv,NULL);
        int off = strftime(timestamp_str,sizeof(timestamp_str),"%d %b %H:%M:%S.",localtime(&tv.tv_sec));
        snprintf(timestamp_str + off, sizeof(timestamp_str) - off,"%03d",(int)tv.tv_usec/1000);

        fprintf(fp,"[%s] %s %s\n", level_to_string[level], timestamp_str, msg);
    }

    fflush(fp);
    if (config->log_path) fclose(fp);
}

void cdb_log(int level, const char *fmt, ...)
{
    va_list ap;
    assert(config);

    if (level > config->log_level) return;

    char msg[LOG_MAX_LEN];

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    do_log(level, msg);
}
