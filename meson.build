project(
  'ccubedb', 'c',
  version : '0.1.0',
  license : 'GPLv3',
  default_options : ['c_std=gnu11']
)

sources = files([
  'cubedb.c',
  'cube.c',
  'filter.c',
  'insert_row.c',
  'log.c',
  'partition.c',
  'sds.c',
])

executable('cubedb', ['server.c'] + sources)

subdir('tests')
