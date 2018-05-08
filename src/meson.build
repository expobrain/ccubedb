sources = files([
  'cdb_cubedb.c',
  'cdb_cube.c',
  'cdb_filter.c',
  'cdb_insert_row.c',
  'cdb_partition.c',
  'cdb_log.c',
  'cdb_config.c',
  'cdb_client.c',
  'cdb_network.c',
  'sds.c',
])

cdb_exec = executable('cubedb', ['cdb_server.c'] + sources)
