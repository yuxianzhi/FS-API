#!/usr/bin/python2
# -*- coding: utf-8 -*-
"""file system api.

An wrapper to compat some common file system.
We can only support hdfs and file protocol.

Standard to use: <protocol>://<url><absolute_path>
    hdfs:
       standard: hdfs://[<ip|hostname>:<port>]<absolute_path>
       example:  hdfs://10.147.20.122:50070/user/ts/test.txt
    local file system:
       standard: file://<absolute_path>
       example:  file:///home/ts/test.txt
"""


from __future__ import print_function

import sys
import os
import re

class FsApi(object):
    """File System API.

    """

    def __init__(self, debug=False):
        """Inits internal states.

        Args:
            debug: Whether to print debug messages, default: False.

        """

        self.DEBUG = debug
        self.url = None
        self.user = None
        self.client = None
        self.fileName = None
        self.protocol = None
        self.options = None
        self.file = None
        self.clientsUsage = {"Client": ('Client(url, root=None, proxy=None, '
                                        'timeout=None, session=None)'),
                             "InsecureClient": "InsecureClient(url, user=None, **kwargs)",
                             "TokenClient": "TokenClient(url, token, **kwargs)",
                             "KerberosClient": "KerberosClient(url)"}
        self.error = {'client': 0,
                      'type': 1,
                      'value': 2}
        self.protocols = ['file', 'hdfs']
        if self.DEBUG:
            print("[DEBUG]\tFS: Init\n")


    # common subfunction
    def getprotocol(self, fileName):
        """Get protocol by using fileName

        Args:
            fileName: file absolute path.
                      - HDFS file example:
                           hdfs://[<ip|hostname>:<port>]<absolute_path>
                      - local file example: file://<absolute_path>

        Returns:
            result: (protocol, absolute_path, ip_port)

        """
        if (not isinstance(fileName, str))
          or (len(fileName.strip()) <= 0):
            print("[ERROR]\tFS: Error\
                   (please provide a valid file name)\n")
            sys.exit(self.error['value'])

        # find protocol
        protocol = None
        protocol_list = re.findall(r"(.+)://.*", fileName)
        if len(protocol_list) > 0:
            protocol = protocol_list[0]
        if (not isinstance(protocol, str)) or (not protocol in self.protocols):
            print("[ERROR]\tFS: Error(please provide a valid protocol(file/hdfs))\n")
            sys.exit(self.error['value'])

        # find url
        url = None
        subFileName = fileName[(len(protocol)+3):]
        if len(subFileName.strip()) <= 0:
            print("[ERROR]\tFS: Error(please provide a valid file name)\n")
            sys.exit(self.error['value'])
        url_list = re.findall(r"(.+:[0-9]+)/.*", subFileName)
        if len(url_list) > 0 and len(url_list[0]) > 0:
            if protocol == 'hdfs':
                url = "http://" + url_list[0]
            else:
                url = url_list[0]

        # find absolute file path
        abs_path = None
        if url:
            abs_path = subFileName[len(url_list[0]):].strip()
        else:
            abs_path = subFileName
        if len(abs_path) == 0:
            print("[ERROR]\tFS: Error(please provide a valid file name %s)\n"
                  % (fileName))
            sys.exit(self.error['value'])
        return (protocol, abs_path, url)


    def gethome(self, protocol):
        """Get home directory

        Args:
            protocol: file system protocol
        Returns:
            home_dir: home directory

        """
        if protocol == 'hdfs':
            if not self.user:
                print(('[ERROR]\tFS: Error(You use HDFS, please'
                       ' first build connection by using connect() method)\n'))
                sys.exit(self.error['client'])
            home_dir = "hdfs:///user/" + self.user

        if protocol == 'file':
            home_dir = "file://" + os.path.expanduser('~')

        return home_dir


    # HDFS build connection
    def getAvailableHDFSClient(self):
        """Get available HDFS client at present

        """
        for client in self.clientsUsage.keys():
            print("%s : %s" % (client, self.clientsUsage[client]))

    def buildHDFSConnect(self, client):
        """ Build from already have connection

        Args:
            client: hdfs client

        """
        from hdfs import Client, InsecureClient, TokenClient
        from hdfs.ext.kerberos import KerberosClient
        if isinstance(client, (Client, InsecureClient, TokenClient, KerberosClient)):
            self.client = client
        else:
            print("[Error]\tFS: Unsupport HDFS client\n")
            sys.exit(self.error['value'])

        if self.DEBUG:
            print("[DEBUG]\tFS: Connect HDFS\n")

    def connectHDFS(self, url, user=None):
        """Creates connection.

        Args:
            url: HDFS url, example http://10.12.0.11:50070
            user: HDFS user, default: `whoami` command return

        """
        if (not isinstance(url, str)) or len(url.strip()) <= 0:
            print("[ERROR]\tFS: Error(Please provide a vaild url, Type: str)\n")
            sys.exit(self.error['value'])

        if not user:
            user = os.system("whoami")
        if (not isinstance(user, str)) or len(user.strip()) <= 0:
            print("[ERROR]\tFS: Error(Please provide a vaild user name, Type: str)\n")
            sys.exit(self.error['value'])

        from hdfs import InsecureClient
        try:
            self.client = InsecureClient(url, user=user)
        except:
            print("[ERROR]\tFS: Can not build connect(url=%s user=%s)\n" % (url, user))
            sys.exit(self.error['error'])

        self.url = url
        self.user = user

        if self.DEBUG:
            print("[DEBUG]\tFS: Connect HDFS(url=%s user=%s)\n"
                  % (url, user))

    def disconnectHDFS(self):
        """Remove connection.

        """
        self.url = None
        self.user = None
        self.client = None

        if self.DEBUG:
            print("[DEBUG]\tFS: Disconnect HDFS(url=%s user=%s)...\n"
                  % (self.url, self.user))


    # file open&close operation
    def open(self, fileName, options):
        """Open file.

        Args:
            fileName: file name
            options: file options

        """
        protocol, abs_path, url = self.getprotocol(fileName)
        self.fileName = abs_path
        self.options = options
        self.protocol = protocol

        if protocol == 'hdfs':
            if (not self.client):
                if (not url) and (not url == self.url):
                    self.connectHDFS(url)
                else:
                    print(('[ERROR]\tFS: Write Error(Please first'
                           ' build connection by using connectHDFS() method)\n'))
                    sys.exit(self.error['client'])
        if protocol == 'file':
            try:
                self.file = open(self.fileName, self.options)
            except:
                print("[ERROR]\tFS: Open local file error(fileName=%s options=%s)\n"
                      % (self.fileName, self.options))
                sys.exit(self.error['value'])

        if self.DEBUG:
            if self.protocol == 'hdfs':
                print("[DEBUG]\tFS: Open HDFS file(url=%s user=%s filename=%s)...\n"
                      % (self.url, self.user, self.fileName))
            if self.protocol == 'file':
                print("[DEBUG]\tFS: Open local file(filename=%s)...\n"
                      % (self.fileName))

    def close(self):
        """Close file.

        """
        # local file system
        if self.protocol == 'file':
            if self.file:
                self.file.close()

        self.fileName = None
        self.protocol = None
        self.options = None
        self.file = None
        if self.DEBUG:
            if self.protocol == 'hdfs':
                print("[DEBUG]\tFS: Close HDFS file(url=%s user=%s filename=%s)...\n"
                      % (self.url, self.user, self.fileName))
            if self.protocol == 'file':
                print("[DEBUG]\tFS: Close local file(filename=%s)...\n"
                      % (self.fileName))


    # file read&write
    def read(self):
        """ Read a file

        Returns:
            line: str|None, file all content

        """
        if not self.fileName:
            print(('[ERROR]\tFS: Can not find read file name'
                   '(you need to use open() method to open file)\n'))
            sys.exit(self.error['value'])

        if self.protocol == 'hdfs':
            if not self.client:
                print(('[ERROR]\tFS: Write Error(Please first build'
                       ' connection by using connect() method)\n'))
                sys.exit(self.error['client'])

            with self.client.read(self.fileName) as reader:
                return reader.read()
        if self.protocol == 'file':
            return self.file.read()

    def readline(self):
        """ Read one line from file

        Returns:
            line: str|None, one line of file

        """
        if not self.fileName:
            print(('[ERROR]\tFS: Can not find read file name'
                   '(you need to use open() method to open file)\n'))
            sys.exit(self.error['value'])

        if self.protocol == 'hdfs':
            if not self.client:
                print(("[ERROR]\tFS: Write Error(Please first build"
                       " connection by using connectHDFS() method)\n"))
                sys.exit(self.error['client'])
            with self.client.read(self.fileName, encoding='utf-8', delimiter='\n') as reader:
                for line in reader:
                    yield line + "\n"
        if self.protocol == 'file':
            yield self.file.readline()

    def readlines(self):
        """ Read lines from file

        Returns:
            lines: list|None, lines array

        """
        if not self.fileName:
            print(("[ERROR]\tFS: Can not find read file name"
                   "(you need to use open() method to open file)\n"))
            sys.exit(self.error['value'])

        if self.protocol == 'hdfs':
            if not self.client:
                print(("[ERROR]\tFS: HDFS Write Error(Please "
                       "first build connection by using connectHDFS() method)\n"))
                sys.exit(self.error['client'])
            lines = []
            with self.client.read(self.fileName, encoding='utf-8', delimiter='\n') as reader:
                for line in reader:
                    lines.append(line+"\n")
            if len(lines) == 0:
                return None
            else:
                return lines
        if self.protocol == 'file':
            return self.file.readlines()

    def writeline(self, line, overwrite=False):
        """Write line to file.

        Args:
            line: string that need to be writen to file.
            overwrite: whether to overwrite exists data, default: False

        """
        if not self.fileName:
            print(("[ERROR]\tFS: Can not find write file name"
                   " (you need to use open() method to open file)\n"))
            sys.exit(self.error['value'])

        if not isinstance(line, str):
            print(("[ERROR]\tFS: Write Type Error(writeline()"
                   " method can only be used to write str type)\n"))
            sys.exit(self.error['type'])

        if not line:
            return

        if self.protocol == 'hdfs':
            if not self.client:
                print(("[ERROR]\tFS: Write Error(Please first build"
                       " connection by using connectHDFS() method)\n"))
                sys.exit(self.error['client'])
            with self.client.write(self.fileName, overwrite=overwrite, encoding='utf-8') as writer:
                writer.write(line)
            return
        if self.protocol == 'file':
            self.file.write(line)
            return

    def writelines(self, lines, overwrite=False):
        """Write line array to file.

        Args:
            content: string or list of string that need to be writen to file.
            overwrite: whether to overwrite exists data, default: False

        """
        if not self.fileName:
            print(("[ERROR]\tFS: Can not find write file name"
                   "(you need to use open() method to open file)\n"))
            sys.exit(self.error['value'])

        if not isinstance(lines, list):
            print(("[ERROR]\tFS: Write Type Error(writelines() method"
                   " can only be used to write string array type)\n"))
            sys.exit(self.error['type'])

        if len(lines) > 0:
            if not isinstance(lines[0], str):
                print(("[ERROR]\tFS: Write Type Error(writelines() method"
                       " can only be used to write string array type)\n"))
                sys.exit(self.error['type'])
        else:
            return

        if self.protocol == 'hdfs':
            if not self.client:
                self.connectHDFS(self.url)

            with self.client.write(self.fileName, overwrite=overwrite, encoding='utf-8') as writer:
                for line in lines:
                    writer.write(line)
            return
        if self.protocol == 'file':
            for line in lines:
                self.file.write(line)
            return

    def write(self, lines, overwrite=False):
        """Write line to file.

        Args:
            lines: string or list of string that need to be writen to file.
            overwrite: whether to overwrite exists data, default: False

        """
        if isinstance(lines, list):
            self.writelines(lines, overwrite)
            return
        if isinstance(lines, str):
            self.writeline(lines, overwrite)
            return
        print(("[ERROR]\tFS: Write Type Error(writelines() method"
               " can only be used to write string array or string)\n"))
        sys.exit(self.error['type'])


    #file transport local <-> remote
    def copy(self, srcFileName, destFileName, overwrite=False, n_threads=1):
        """Copy file(local <-> remote, local -> local, remote -> remote)

        Args:
            srcFileName: source file name
            destFileName: destination file name
            overwrite: whether to overwrite, default: False
            n_threads: parallel threads, default: 1

        """
        if not self.exists(srcFileName):
            print("[ERROR]\tFS: Error(plesase provide a valid source file name)\n")
            sys.exit(self.error['value'])
        if self.exists(destFileName) and (not overwrite):
            print(("[ERROR]\tFS: Error(you want to overwrite a exists file."
                   " plesase consider to use overwrite parameter)\n"))
            sys.exit(self.error['value'])

        protocol0, abs_path0, url0 = self.getprotocol(srcFileName)
        protocol1, abs_path1, url1 = self.getprotocol(destFileName)
        if protocol0 == 'hdfs':
            if not self.client:
                if (not url0) and (not url0 == self.url):
                    self.connectHDFS(url0)
                else:
                    print(("[ERROR]\tFS: Write Error(Please first"
                           " build connection by using connectHDFS() method)\n"))
                    sys.exit(self.error['client'])

            # hdfs -> hdfs
            if protocol1 == 'hdfs':
                print("[ERROR]\tFS: Error(hdfs -> hdfs is not support at present)\n")
                sys.exit(self.error['value'])
                return

            # hdfs -> file
            if protocol1 == 'file':
                self.client.download(abs_path0,
                                     abs_path1,
                                     overwrite=overwrite,
                                     n_threads=n_threads)
                return

        if protocol0 == 'file':
            # file -> hdfs
            if protocol1 == 'hdfs':
                if not self.client:
                    if (not url1) and (not url1 == self.url):
                        self.connectHDFS(url1)
                    else:
                        print(("[ERROR]\tFS: Write Error(Please first build"
                               " connection by using connectHDFS() method)\n"))
                        sys.exit(self.error['client'])
                self.client.upload(abs_path1,
                                   abs_path0,
                                   overwrite=overwrite,
                                   n_threads=n_threads)
                return

            # file -> file
            if protocol1 == 'file':
                command = "cp -r " + abs_path0 + " " + abs_path1
                os.system(command)
                return


    # file operation
    def remove(self, fileName, recursive=False):
        """Delete file

        Args:
            fileName: file name
            recursive: whether to remove recursively
        """
        if not self.exists(fileName):
            return

        protocol, abs_path, url = self.getprotocol(fileName)

        if protocol == 'hdfs':
            if not self.client:
                if (not url) and (not url == self.url):
                    self.connectHDFS(url)
                else:
                    print(("[ERROR]\tFS: Write Error(Please first build"
                           " connection by using connectHDFS() method)\n"))
                    sys.exit(self.error['client'])

            self.client.delete(abs_path, recursive)
        if protocol == 'file':
            if os.path.isfile(abs_path):
                os.remove(abs_path)
            else:
                if os.path.isdir(abs_path) and recursive:
                    os.rmdir(abs_path)
        if self.DEBUG:
            if protocol == 'hdfs':
                print("[DEBUG]\t\tFS: Remove HDFS file %s...\n" % (abs_path))
            if protocol == 'file':
                print("[DEBUG]\t\tFS: Remove local file %s...\n" % (abs_path))

    def exists(self, fileName):
        """Get file status

        Args:
            fileName: file name

        Returns:
            status: True, if file exists; False, if file doesn't exists

        """
        protocol, abs_path, url = self.getprotocol(fileName)

        if protocol == 'hdfs':
            if not self.client:
                if (not url) and (not url == self.url):
                    self.connectHDFS(url)
                else:
                    print(("[ERROR]\tFS: Write Error(Please first"
                           " build connection by using connectHDFS() method)\n"))
                    sys.exit(self.error['client'])

            if self.client.status(abs_path, strict=False):
                return True
            else:
                return False
        if protocol == 'file':
            return os.path.exists(abs_path)

    def listdir(self, dirName):
        """Get all file in a directory

        Args:
            dirName: directory name

        Returns:
            files: file in the directory

        """
        protocol, abs_path, url = self.getprotocol(dirName)

        if protocol == 'hdfs':
            if not self.client:
                if (not url) and (not url == self.url):
                    self.connectHDFS(url)
                else:
                    print(("[ERROR]\tFS: Write Error(Please first"
                           " build connection by using connectHDFS() method)\n"))
                    sys.exit(self.error['client'])
            return self.client.list(abs_path)
        if protocol == 'file':
            return os.listdir(abs_path)
