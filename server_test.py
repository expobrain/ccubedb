#!/usr/bin/env python
from __future__ import unicode_literals, print_function

import unittest
import subprocess
import shlex
import socket
import time


start_port = 1500

REPLY_OK = "0\n"


class CubeDBTestBase(unittest.TestCase):

    EXECUTABLE = "cubedb"

    def setUp(self):
        global start_port
        start_port += 1

        cmd = "./{cmd} --port {port} --log-level 0".format(
            cmd=self.EXECUTABLE,
            port=str(start_port)
        )
        self.process = subprocess.Popen(shlex.split(cmd))

        retries = 3
        while retries > 0:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                self.sock.connect(('localhost', start_port))
                break
            except Exception:
                if retries <= 0:
                    raise
                time.sleep(0.1)
                retries -= 1

        self.sock_file = self.sock.makefile()

    def send(self, msg):
        self.sock.sendall(msg)

    def readline(self):
        return self.sock_file.readline()

    def sendline(self, msg):
        if msg[-1] != "\n":
            msg += "\n"
        self.send(msg)

    def sendwithok(self, msg):
        self.sendline(msg)
        assert self.readline() == REPLY_OK

    def sendwitherr(self, msg):
        self.sendline(msg)
        reply = self.readline()
        assert reply != REPLY_OK
        assert reply.startswith("-")

    def sendwithcount(self, msg):
        self.sendline(msg)
        reply = self.readline()
        assert not reply.startswith("-")
        return int(reply)

    def sendwithlines(self, msg):
        self.sendline(msg)
        linum = int(self.readline())
        assert linum >= 0

        lines = []
        for _ in range(linum):
            lines.append(self.readline().strip())
        return lines

    def sendwithmap(self, msg):
        lines = self.sendwithlines(msg)
        result = {}
        for line in lines:
            line_parts = line.split()
            assert len(line_parts) == 2
            result[line_parts[0]] = line_parts[1]
        return result

    def sendwithmapmap(self, msg):
        self.sendline(msg)
        mapnum = int(self.readline())
        assert mapnum >= 0

        result = {}
        for _ in range(mapnum):
            top_key = self.readline().strip()
            result[top_key] = {}

            innermapnum = int(self.readline())
            assert innermapnum >= 0

            for _ in range(innermapnum):
                line = self.readline().strip()
                line_parts = line.split()
                assert len(line_parts) == 2
                result[top_key][line_parts[0]] = line_parts[1]

        return result

    def tearDown(self):
        self.sock.close()
        self.process.kill()
        self.process.wait()


class CubeDBTest(CubeDBTestBase):

    def test_unknown(self):
        self.sendline("RANDOM CMD")
        reply = self.readline()
        assert reply != REPLY_OK
        assert reply.startswith("-")

    def test_wrong_arg_um(self):
        self.sendline("QUIT arg")
        reply = self.readline()
        assert reply != REPLY_OK
        assert reply.startswith("-")

    def test_quit(self):
        self.sendline("QUIT")
        reply = self.readline()
        assert reply == REPLY_OK

    def test_cube(self):
        # Does not exist
        self.sendline("CUBE cube")
        reply = self.readline()
        assert reply != REPLY_OK

        # Add cube and check without partitions
        self.sendwithok("ADDCUBE cube")
        lines = self.sendwithlines("CUBE cube")
        assert len(lines) == 0

        # Add two partitions, check the results
        self.sendwithok("INSERT cube p1 a=1 1")
        self.sendwithok("INSERT cube p2 a=1 1")

        lines = self.sendwithlines("CUBE cube")
        assert len(lines) == 2
        for line in lines:
            assert line in ("p1", "p2")

    def test_add_cube(self):
        # Just add a cube
        self.sendline("ADDCUBE cube")
        reply = self.readline()
        assert reply == REPLY_OK

        # Already exists
        self.sendline("ADDCUBE cube")
        reply = self.readline()
        assert reply != REPLY_OK
        assert reply.startswith("-")

    def test_del_cube(self):
        # Does not exist
        self.sendline("DELCUBE cube")
        reply = self.readline()
        assert reply != REPLY_OK
        assert reply.startswith("-")

        # Just add a cube
        self.sendline("ADDCUBE cube")
        reply = self.readline()
        assert reply == REPLY_OK

        # Delete
        self.sendline("DELCUBE cube")
        reply = self.readline()
        assert reply == REPLY_OK

        # Does not exist
        self.sendline("DELCUBE cube")
        reply = self.readline()
        assert reply != REPLY_OK

    def test_del_partition(self):
        # cube does not exist
        self.sendwitherr("DELPART cube part")

        self.sendwithok("ADDCUBE cube")

        # the partition does not exist
        self.sendwitherr("DELPART cube part")

        self.sendwithok("INSERT cube part a=1 1")
        partitions = self.sendwithlines("CUBE cube")
        assert partitions == ["part"]

        # wrong arg number
        self.sendwitherr("DELPART cube")

        # delete the partition and check if there's anything left
        self.sendwithok("DELPART cube part")

        partitions = self.sendwithlines("CUBE cube")
        assert partitions == []

    def test_del_partition_from_to(self):
        # cube does not exist
        self.sendwitherr("DELPART cube part1 part2")

        self.sendwithok("ADDCUBE cube")

        # partition does not exist but this cmd does not care
        self.sendwithok("DELPART cube part1 part3")

        self.sendwithok("INSERT cube part1 a=1 1")
        partitions = self.sendwithlines("CUBE cube")
        assert partitions == ["part1"]

        # delete out of range
        self.sendwithok("DELPART cube part2 part3")
        partitions = self.sendwithlines("CUBE cube")
        assert partitions == ["part1"]

        # delete it, at last
        self.sendwithok("DELPART cube part1 part3")
        partitions = self.sendwithlines("CUBE cube")
        assert partitions == []

    def test_cubes(self):
        # Nothing yet
        self.sendline("CUBES")
        reply = self.readline()
        assert not reply.startswith("-")
        assert int(reply.strip()) == 0

        # A few cubes
        cubes = "cube1", "cube2", "cube3"
        for cube in cubes:
            self.sendwithok("ADDCUBE {}".format(cube))

        # Check the number of cubes found
        lines = self.sendwithlines("CUBES")
        assert len(lines) == len(cubes)
        for line in lines:
            assert line in cubes

    def test_insert(self):
        # Insert without a cube - fail
        self.sendline("INSERT cube p1 a=1 1")
        reply = self.readline()
        assert reply != REPLY_OK
        assert reply.startswith("-")

        # Add a cube and repeat with a wrong arg
        self.sendwithok("ADDCUBE cube")
        self.sendline("INSERT cube p1 a 1")
        reply = self.readline()
        assert reply != REPLY_OK
        assert reply.startswith("-")

        # Now, correct args
        self.sendwithok("INSERT cube p1 a=1 1")
        self.sendwithok("INSERT cube p2 a=1&b=1 1")

        lines = self.sendwithlines("CUBE cube")
        assert len(lines) == 2
        for line in lines:
            assert line in ("p1", "p2")

    def test_count(self):
        # Cube does not exist
        self.sendwitherr("COUNT cube p1 p9")

        # Empty cube
        self.sendwithok("ADDCUBE cube")

        lines = self.sendwithlines("COUNT cube p1 p9")
        assert 0 == len(lines)

        # Insert some data into a partition
        self.sendwithok("INSERT cube p2 a=1 1")
        self.sendwithok("INSERT cube p2 a=2 2")

        count = self.sendwithcount("COUNT cube p1 p9")
        assert 3 == count

        count = self.sendwithcount("COUNT cube p3 p9")
        assert 0 == count

        # One more partition
        self.sendwithok("INSERT cube p3 b=1 1")

        count = self.sendwithcount("COUNT cube p3 p9")
        assert 1 == count
        count = self.sendwithcount("COUNT cube p1 p9")
        assert 4 == count

    def test_count_grouped(self):
        # Empty cube
        self.sendwithok("ADDCUBE cube")

        # Nothing here yet
        value_to_count = self.sendwithmap("COUNT cube p1 p9 null a")
        assert 0 == len(value_to_count)

        # Insert some data into a partition
        self.sendwithok("INSERT cube p1 a=val1 1")
        self.sendwithok("INSERT cube p2 a=val1 2")
        self.sendwithok("INSERT cube p2 a=val2 4")

        # Check that mapping is all right
        value_to_count = self.sendwithmap("COUNT cube p1 p9 null a")
        assert value_to_count == {'val1': '3', 'val2': '4'}

    def test_count_filter(self):
        # The cube does not exist
        self.sendwitherr("COUNT cube p1 p9 a=1")

        # Empty cube
        self.sendwithok("ADDCUBE cube")

        count = self.sendwithcount("COUNT cube p1 p9 a=1")
        assert 0 == count

        # Insert some data into a partition
        self.sendwithok("INSERT cube p2 a=1 1")
        self.sendwithok("INSERT cube p2 a=2 2")

        count = self.sendwithcount("COUNT cube p1 p9 a=1")
        assert 1 == count

        count = self.sendwithcount("COUNT cube p3 p9 a=1")
        assert 0 == count

        # One more partition
        self.sendwithok("INSERT cube p3 a=1 1")

        count = self.sendwithcount("COUNT cube p3 p9 a=1")
        assert 1 == count
        count = self.sendwithcount("COUNT cube p1 p9 a=1")
        assert 2 == count

    def test_count_filter_multiple_column_values(self):
        # Empty cube
        self.sendwithok("ADDCUBE cube")

        # Insert some data into a partition
        self.sendwithok("INSERT cube p2 a=1 1")
        self.sendwithok("INSERT cube p2 a=2 2")
        self.sendwithok("INSERT cube p2 a=3&b=2 3")

        # Count a single row
        count = self.sendwithcount("COUNT cube p1 p9 a=1")
        assert 1 == count

        # Count everything with a=[1,2]
        count = self.sendwithcount("COUNT cube p1 p9 a=1&a=2")
        assert 3 == count

    def test_pcount(self):
        # The cube does not exist
        self.sendwitherr("PCOUNT cube p1 p9")

        # Empty cube
        self.sendwithok("ADDCUBE cube")

        partition_to_count = self.sendwithmap("PCOUNT cube p1 p9")
        assert 0 == len(partition_to_count)

        # Insert some data into a partition
        self.sendwithok("INSERT cube p2 a=1 1")
        self.sendwithok("INSERT cube p2 a=2 2")

        partition_to_count = self.sendwithmap("PCOUNT cube p1 p9")
        assert 1 == len(partition_to_count)
        assert "p2" in partition_to_count
        assert '3' == partition_to_count.get("p2")

        partition_to_count = self.sendwithmap("PCOUNT cube p3 p9")
        assert 0 == len(partition_to_count)

        # One more partition
        self.sendwithok("INSERT cube p3 a=1 1")

        partition_to_count = self.sendwithmap("PCOUNT cube p3 p9")
        assert 1 == len(partition_to_count)
        partition_to_count = self.sendwithmap("PCOUNT cube p1 p9")
        assert 2 == len(partition_to_count)
        assert {"p2": "3", "p3": "1"} == partition_to_count

    def test_pcount_filter(self):
        # The cube does not exist
        self.sendwitherr("PCOUNT cube p1 p9 a=1")

        # Empty cube
        self.sendwithok("ADDCUBE cube")

        # Nothing here yet
        partition_to_count = self.sendwithmap("PCOUNT cube p1 p9 a=1")
        assert 0 == len(partition_to_count)

        # Insert some data into a partition
        self.sendwithok("INSERT cube p2 a=1 1")
        self.sendwithok("INSERT cube p2 a=2 2")

        partition_to_count = self.sendwithmap("PCOUNT cube p1 p9 a=1")
        assert 1 == len(partition_to_count)
        assert {"p2": "1"} == partition_to_count

        partition_to_count = self.sendwithmap("PCOUNT cube p1 p9 a=2")
        assert 1 == len(partition_to_count)
        assert {"p2": "2"} == partition_to_count

        partition_to_count = self.sendwithmap("PCOUNT cube p3 p9 a=1")
        assert 0 == len(partition_to_count)

        # One more partition
        self.sendwithok("INSERT cube p3 a=1 1")

        partition_to_count = self.sendwithmap("PCOUNT cube p3 p9 a=1")
        assert 1 == len(partition_to_count)
        partition_to_count = self.sendwithmap("PCOUNT cube p1 p9 a=1")
        assert 2 == len(partition_to_count)
        assert {"p2": "1", "p3": "1"} == partition_to_count

    def test_pcount_grouped(self):
        # Empty cube
        self.sendwithok("ADDCUBE cube")

        # Nothing here yet
        partition_to_value_to_count = self.sendwithmapmap("PCOUNT cube p1 p9 null a")
        assert 0 == len(partition_to_value_to_count)

        # Insert some data into a partition
        self.sendwithok("INSERT cube p1 a=val1 1")
        self.sendwithok("INSERT cube p2 a=val1 2")
        self.sendwithok("INSERT cube p2 a=val2 4")

        # Check the mapping
        partition_to_value_to_count = self.sendwithmapmap("PCOUNT cube p1 p9 null a")
        assert partition_to_value_to_count == {'p1': {'val1': '1'}, 'p2': {'val1': '2', 'val2': '4'}}

    def test_help(self):
        # This is help so no real testing here
        assert self.sendwithlines("HELP")


if __name__ == '__main__':
    unittest.main()
