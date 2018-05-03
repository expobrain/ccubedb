
test_includes = include_directories('../')
test_sources = [
  '../cubedb.c',
  '../cube.c',
  '../filter.c',
  '../insert_row.c',
  '../log.c',
  '../partition.c',
  '../sds.c',
]
test_lib = static_library('cubedb', test_sources)

test_e = executable(
  'slist-test', 'slist-test.c', link_with : test_lib, include_directories : test_includes
)
test('slist test', test_e)

test_e = executable(
  'htable-test', 'htable-test.c', link_with : test_lib, include_directories : test_includes
)
test('htable test', test_e)

test_e = executable(
  'cubedb-test', 'cubedb-test.c', link_with : test_lib, include_directories : test_includes
)
test('cubedb test', test_e)

test_e = executable(
  'cube-test', 'cube-test.c', link_with : test_lib, include_directories : test_includes
)
test('cube test', test_e)
