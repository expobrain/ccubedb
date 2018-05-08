
test_includes = include_directories('../src/')

test_lib = static_library('cubedb', sources)

test_e = executable(
  'slist-test', 'slist-test.c', link_with : test_lib, include_directories : test_includes
)
test('slist-test', test_e)

test_e = executable(
  'htable-test', 'htable-test.c', link_with : test_lib, include_directories : test_includes
)
test('htable-test', test_e)

test_e = executable(
  'cubedb-test', 'cubedb-test.c', link_with : test_lib, include_directories : test_includes
)
test('cubedb-test', test_e)

test_e = executable(
  'cube-test', 'cube-test.c', link_with : test_lib, include_directories : test_includes
)
test('cube-test', test_e)

test_e = find_program(
  'server-test.py'
)
test('functional-test', test_e, env : ['CDB_EXECUTABLE=' + cdb_exec.full_path()])
