
test_includes = include_directories('../src/')

test_lib = static_library('cubedb', sources)

unit_test_names = [
  'slist-test',
  'htable-test',
  'cubedb-test',
  'cube-test',
  'partition-test',
]

foreach test_name : unit_test_names
  test_e = executable(test_name, test_name + '.c', link_with : test_lib, include_directories : test_includes)
  test(test_name, test_e)
endforeach

test_e = find_program(
  'server-test.py'
)

test_cdb_exec = executable('test_cubedb', server_sources)
test('functional-test', test_e, env : ['CDB_EXECUTABLE=' + test_cdb_exec.full_path()])
