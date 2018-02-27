CC      = gcc-7
LD      = gcc-7
CFLAGS  = -MMD -MP -g -Wall -Wextra -Werror -Wshadow -std=gnu11
# CFLAGS  += -fsanitize=undefined
# CFLAGS  += -fsanitize=address -fsanitize=leak
LDFLAGS  =-Wl,--no-undefined
# LDLIBS += -lubsan
# LDLIBS += -lasan

OBJ = cubedb.o cube.o partition.o sds.o insert_row.o filter.o log.o
ALL_OBJ = server.o $(OBJ)
TESTS = cubedb-test cube-test slist-test htable-test
DEPS = $(ALL_OBJ:.o=.d)

cubedb: $(ALL_OBJ)

test: unit-test external-test

unit-test: $(TESTS)
	for test in $(TESTS); do \
		./$$test; \
	done

external-test: cubedb
	./server_test.py

cubedb-test: cubedb-test.o $(OBJ)

cube-test: cube-test.o $(OBJ)

slist-test: slist-test.o

htable-test: htable-test.o

sds.o: sds.c
	$(CC) -Wall -std=c99 -pedantic -O2 -c $<

clean:
	$(RM) *.o
	$(RM) $(TESTS)
	$(RM) core
	$(RM) $(DEPS) $(TESTS:=.d)

cppcheck:
	cppcheck --enable=all -q -i sds.c .

tags:
	gtags .

-include $(DEPS)

.PHONY: clean cppcheck tags test unit-test external-test
