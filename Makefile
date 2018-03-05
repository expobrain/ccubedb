CC      = gcc-7
LD      = gcc-7

# Default build
CFLAGS  = -MMD -MP -Wall -Wextra -Werror -Wshadow -std=gnu11
LDFLAGS =-Wl,--no-undefined

ifdef FAST
# Fast build
CFLAGS += -flto -march=native -ffast-math -O2
else ifdef TINY
# Tiny binary
CFLAGS += -flto -Os -ffast-math
else ifdef UBSAN
# Check undefined behaviour
CFLAGS  += -g -fsanitize=undefined
LDLIBS += -lubsan
else ifdef ASAN
# Check leaks, etc
CFLAGS  += -g -fsanitize=address -fsanitize=leak
LDLIBS += -lasan
else
# Debug symbols installed by default
CFLAGS += -g
endif

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
