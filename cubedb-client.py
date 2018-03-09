#!/usr/bin/env python
from __future__ import print_function, unicode_literals

import shlex
import argparse
import socket
import traceback
from pprint import pprint


class CubeDBError(Exception):
    pass


REPLY_OK = "0"
REPLY_ERR = "-3"              # Command generic error  */
REPLY_ERR_NOT_FOUND = "-4"            # Command not found */
REPLY_ERR_WRONG_ARG = "-5"            # Command argument is wrong */
REPLY_ERR_WRONG_ARG_NUM = "-6"        # Command argument number is wrong */
REPLY_ERR_MALFORMED_ARG = "-7"        # Command argument is contains non-graphic symbols*/
REPLY_ERR_OBJ_NOT_FOUND = "-8"        # Command object not found */
REPLY_ERR_OBJ_EXISTS = "-9"           # Command object already exists */

ERROR_TO_MSG = {
    REPLY_ERR: "Command generic error",
    REPLY_ERR_NOT_FOUND: "Command not found",
    REPLY_ERR_WRONG_ARG: "Command argument is wrong",
    REPLY_ERR_WRONG_ARG_NUM: "Command argument number is wrong",
    REPLY_ERR_MALFORMED_ARG: "Command argument contains non-graphic symbols",
    REPLY_ERR_OBJ_NOT_FOUND: "Command object not found",
    REPLY_ERR_OBJ_EXISTS: "Command object already exists",
}


class CubeDB(object):

    def __init__(self, host, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))
        self.sock_file = self.sock.makefile()

        self.cmd_table = {
            'PING': self.cmd_ping,
            'HELP': self.cmd_help,
            'QUIT': self.cmd_quit,
            'CUBES': self.cmd_cubes,
            'ADDCUBE': self.cmd_addcube,
            'CUBE': self.cmd_cube,
            'DELCUBE': self.cmd_delcube,
            'PART': self.cmd_part,
            'DELPART': self.cmd_delpart,
            'INSERT': self.cmd_insert,
            'COUNT': self.cmd_count,
            'PCOUNT': self.cmd_pcount,
        }

    def execute_from_line(self, line):
        parts = [p.strip() for p in shlex.split(line)]
        if not len(parts) >= 1:
            raise CubeDBError("Need at least one argument")
        cmd = parts[0].upper()
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
        reply = self.readline().strip()
        if reply == REPLY_OK:
            return True
        elif reply in ERROR_TO_MSG:
            raise CubeDBError("Expected REPLY_OK (0), got: {} ({})".format(ERROR_TO_MSG[reply], reply))
        else:
            raise CubeDBError("Expected REPLY_OK (0), got: {}".format(reply))

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
            result[" ".join(line_parts[:-1])] = line_parts[-1]
        return result

    def readmapmap(self):
        mapnum = self.readcount()

        map_to_map_to_count = {}
        for _ in range(mapnum):
            top_key = self.readline().strip()
            map_to_map_to_count[top_key] = {}

            innermapnum = self.readcount()
            for _ in range(innermapnum):
                line = self.readline().strip()
                line_parts = line.split()
                assert len(line_parts) == 2
                map_to_map_to_count[top_key][line_parts[0]] = line_parts[1]

        return map_to_map_to_count

    def readmaplist(self):
        mapnum = self.readcount()

        map_to_list = {}
        for _ in range(mapnum):
            top_key = self.readline().strip()
            map_to_list[top_key] = []

            listsize = self.readcount()
            for _ in range(listsize):
                line = self.readline().strip()
                map_to_list[top_key].append(line)

        return map_to_list

    def cmd_ping(self):
        self.sendline("PING")
        return self.readline()

    def cmd_cubes(self):
        self.sendline("CUBES")
        return self.readlines()

    def cmd_addcube(self, name):
        self.sendline("ADDCUBE '{}'".format(name))
        return self.readok()

    def cmd_cube(self, name):
        self.sendline("CUBE '{}'".format(name))
        return self.readlines()

    def cmd_delcube(self, name):
        self.sendline("DELCUBE '{}'".format(name))
        return self.readok()

    def cmd_part(self, name, _from=None, to=None):
        cmd = "PART '{}'".format(name)
        if _from is not None:
            cmd += " '{}'".format(_from)
        if to is not None:
            cmd += " '{}'".format(to)
        self.sendline(cmd)
        return self.readmaplist()

    def cmd_delpart(self, name, _from, to=''):
        self.sendline("DELPART '{}' '{}' '{}'".format(name, _from, to))
        return self.readok()

    def _column_values_to_str(self, columnvalues):
        if isinstance(columnvalues, dict):
            return "&".join("{}={}".format(k, v) for k, v in columnvalues.itervalues())
        else:
            return columnvalues

    def cmd_insert(self, name, partition, columnvalues, count):
        self.sendline("INSERT '{}' '{}' '{}' '{}'".format(name, partition,
                                                          self._column_values_to_str(columnvalues),
                                                          count))
        return self.readok()

    def cmd_count(self, name, _from='', to='', columnvalues='', groupcolumn=''):
        self.sendline("COUNT '{}' '{}' '{}' '{}' '{}'".format(name, _from, to,
                                                              self._column_values_to_str(columnvalues),
                                                              groupcolumn))
        if groupcolumn:
            return self.readmap()
        else:
            return self.readcount()

    def cmd_pcount(self, name, _from='', to='', columnvalues='', groupcolumn=''):
        self.sendline("PCOUNT '{}' '{}' '{}' '{}' '{}'".format(name, _from, to,
                                                               self._column_values_to_str(columnvalues),
                                                               groupcolumn))
        if groupcolumn:
            return self.readmapmap()
        else:
            return self.readmap()

    def cmd_help(self):
        self.sendline("HELP")
        return self.readlines()

    def cmd_quit(self):
        self.sendline("QUIT")
        return self.readok()


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
    parser.add_argument('--silent', action='store_true',
                        help='Do not print cmds to be executed, and cmd execution results')
    args = parser.parse_args()

    cdb = CubeDB(args.host, args.port)

    for line in args.source:
        if not line.strip():
            continue
        if line.startswith('#'):
            continue
        if not args.silent:
            print(line, end='')
        try:
            cmd_result = cdb.execute_from_line(line)
            if not args.silent:
                pprint(cmd_result, indent=4)
        except CubeDBError as e:
            print("Command '{}' failed with server error: '{}'".format(line.strip(), e.message))
            if not args.ignore_errors:
                raise
        except Exception as e:
            print("Command '{}' failed with exception".format(line.strip()), traceback.format_exc(e))
            if not args.ignore_errors:
                raise


if __name__ == '__main__':
    main()
