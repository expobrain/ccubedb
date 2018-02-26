#!/usr/bin/env python
from __future__ import print_function, unicode_literals

import argparse
import socket
import traceback


class CubeDBError(Exception):
    pass


class CubeDB(object):

    REPLY_OK = "0\n"

    def __init__(self, host, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))
        self.sock_file = self.sock.makefile()

        # TODO: PCOUNT
        self.cmd_table = {
            'CUBES': self.cmd_cubes,
            'ADDCUBE': self.cmd_addcube,
            'CUBE': self.cmd_cube,
            'DELCUBE': self.cmd_delcube,
            'DELPART': self.cmd_delpart,
            'INSERT': self.cmd_insert,
            'COUNT': self.cmd_count,
        }

    def execute_from_line(self, line):
        parts = [p.strip() for p in line.split()]
        if not len(parts) >= 1:
            raise CubeDBError("Need at least one argument")
        cmd = parts[0]
        args = parts[1:]
        if cmd not in self.cmd_table:
            raise CubeDBError("Unknown command: {}".format(cmd))
        return self.cmd_table[cmd](*args)

    def sendline(self, cmd_line):
        if cmd_line[-1] != "\n":
            cmd_line += "\n"
        self.sock.sendall(cmd_line)

    def readline(self):
        return self.sock_file.readline()

    def readok(self):
        reply = self.readline()
        if not reply == self.REPLY_OK:
            raise CubeDBError("Expected REPLY_OK, got: {}".format(reply))
        return True

    def readcount(self):
        return int(self.readline())

    def readlines(self):
        linum = self.readcount()
        return [self.readline().strip() for line in range(linum)]

    def readmap(self):
        lines = self.readlines()
        result = {}
        for line in lines:
            line_parts = line.split()
            assert len(line_parts) == 2
            result[line_parts[0]] = line_parts[1]
        return result

    def cmd_cubes(self):
        self.sendline("CUBES")
        return self.readlines()

    def cmd_addcube(self, name):
        self.sendline("ADDCUBE {}".format(name))
        return self.readok()

    def cmd_cube(self, name):
        self.sendline("CUBE {}".format(name))
        return self.readlines()

    def cmd_delcube(self, name):
        self.sendline("DELCUBE {}".format(name))
        return self.readok()

    def cmd_delpart(self, name, _from, to=''):
        self.sendline("DELPART {} {} {}".format(name, _from, to))
        return self.readok()

    def _column_values_to_str(self, columnvalues):
        if isinstance(columnvalues, dict):
            return "&".join("{}={}".format(k, v) for k, v in columnvalues.itervalues())
        else:
            return columnvalues

    def cmd_insert(self, name, partition, columnvalues, count):
        self.sendline("INSERT {} {} {} {}".format(name, partition,
                                                  self._column_values_to_str(columnvalues),
                                                  count))
        return self.readok()

    def cmd_count(self, name, _from, to, columnvalues='', groupcolumn=''):
        self.sendline("COUNT {} {} {} {} {}".format(name, _from, to,
                                                    self._column_values_to_str(columnvalues),
                                                    groupcolumn))
        if groupcolumn:
            return self.readmap()
        else:
            return self.readcount()


def main():
    parser = argparse.ArgumentParser(description='A simplistic CubeDB Python client')
    parser.add_argument('source', type=argparse.FileType('rU', bufsize=1),
                        help='A path to a file to load commands from or "-" for reading from STDIN')
    parser.add_argument('--host', default='localhost',
                        help='CubeDB instance host (localhost by default)')
    parser.add_argument('--port', default=1985, type=int,
                        help='CubeDB instance port (1985 by default)')
    parser.add_argument('--ignore-errors', action='store_true',
                        help='Do not abort execution on command errors')
    args = parser.parse_args()

    cdb = CubeDB(args.host, args.port)

    for line in args.source:
        if not line.strip():
            continue
        if line.startswith('#'):
            continue
        print(line, end='')
        try:
            print(cdb.execute_from_line(line))
        except CubeDBError, e:
            if not args.ignore_errors:
                raise
            print("Failed to execute '{}' with exception".format(line.strip()), traceback.format_exc(e))


if __name__ == '__main__':
    main()
