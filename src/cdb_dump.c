#define _GNU_SOURCE
#include <ftw.h>

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "cdb_dump.h"
#include "cdb_defs.h"
#include "cdb_alloc.h"
#include "slist.h"

static bool ends_with(const char *string, const char *suffix)
{
    if( ! string || ! suffix )
        return false;
    size_t string_len = strlen(string);
    size_t suffix_len = strlen(suffix);
    return strcmp(string + string_len - suffix_len, suffix) == 0;
}

static void load_cube_file(const char *dump_file)
{

}

int cdb_load_dump(const char *dump_dir)
{
    slist_t *cube_file_list = slist_create();
    defer {
        slist_for_each(node, cube_file_list) {
            char *file_path = slist_data(node);
            free(file_path);
        }
        slist_destroy(cube_file_list);
    }

    int find_cubes(const char *fpath, const struct stat *sb,
                   int typeflag, struct FTW *ftwbuf) {
        (void)sb; (void) ftwbuf;
        if (typeflag != FTW_F)
            return 0;

        if (!ends_with(fpath, ".cdb"))
            return 0;

        slist_append(cube_file_list, strdup(fpath));
        return 0;
    }

    if (0 != nftw(dump_dir, find_cubes, 200, 0)) {
        perror("nftw");
        exit(EXIT_FAILURE);
    }

    slist_for_each(node, cube_file_list)
        load_cube_file(slist_data(node));

    return slist_size(cube_file_list);
}
