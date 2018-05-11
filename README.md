# CCubedb

A reimplementation of [CubeDB](https://github.com/cubedb/cubedb) in C, still work-in-progress.

## Building

Currently CCubeDB is developed on Linux using GCC 7 and glibc-2.23, the code is not tested on other
platforms.

CCubeDB uses [Meson](http://mesonbuild.com/) for building the project. Follow the link for Meson
installation instructions.

Otherwise building is simple:

```bash
> git clone https://github.com/vkazanov/ccubedb
...
> cd ccubedb
> meson release --buildtype=release
...
> cd release
> ninja
...
```

Having built the project it should be easy to launch CCubeDB:

```bash
# in ccubedb/release
> ./cubedb --port 1985
[INFO] 11 May 10:36:32.864 Waiting for connections...
```

To test CCubeDB open another terminal and :

```bash
> echo "HELP" | nc localhost 1985
... list of available CCubeDB commands should follow
```

## Running Tests

CCubeDB comes with a set of unit and functional tests. Tests require Python2.7+.

```bash
# in ccubedb/release
> meson test
... test output
```
