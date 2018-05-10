#ifndef CDB_DUMP_H
#define CDB_DUMP_H

#include "cdb_cubedb.h"

int cdb_load_dump(const char *dump_dir, cdb_cubedb *cdb);
int cdb_do_dump(const char *dump_dir, cdb_cubedb *cdb);

#endif //CDB_DUMP_H
