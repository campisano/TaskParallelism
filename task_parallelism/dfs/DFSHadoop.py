#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import os
import sys
from ..utils.os_utils import runCommand
from ..utils.string_utils import toString
from DistributedFileSistem import DistributedFileSistem
from DistributedFileSistem import DistributedFileSistemFactory


class DFSHadoopFactory(DistributedFileSistemFactory):

    def __init__(
        self
    ):
        ####
        # Defining environment
        if not os.getenv("USER"):
            raise Exception("Can not find env variable 'USER'.")

        self.user = os.getenv("USER")

        if not os.getenv("HADOOP_HOME"):
            raise Exception("Can not find env variable 'HADOOP_HOME'.")

        self.hadoop_home = os.getenv("HADOOP_HOME")

        ####
        # Define initial vars
        self.hadoop_user_base_path = os.path.join("/user", self.user)
        self.hadoop_cmd = os.path.join(self.hadoop_home, "bin", "hadoop")
        self.hadoop_blocksize = 4194304     # 4MB

    def create(
        self
    ):
        return DFSHadoop(
            self.hadoop_user_base_path,
            self.hadoop_cmd,
            self.hadoop_blocksize)


class DFSHadoop(DistributedFileSistem):

    def __init__(
        self,
        _hadoop_user_base_path,
        _hadoop_cmd,
        _hadoop_blocksize
    ):
        self.hadoop_user_base_path = _hadoop_user_base_path
        self.hadoop_cmd = _hadoop_cmd
        self.hadoop_blocksize = _hadoop_blocksize

        ####
        # Setup Hadoop log
        home = os.environ["HOME"]
        hadoop_log_dir = os.path.join(home, "hadoop", "log")
        runCommand("mkdir -p " + hadoop_log_dir)
        os.environ["HADOOP_LOG_DIR"] = hadoop_log_dir

    def downloadDataFromDFS(
        self,
        _dfs_path,
        _local_path,
        _keep_going=False,
        _verbose=True,
        _log=sys.stdout
    ):
        result = runCommand(
            self.hadoop_cmd + " fs" +
            " -copyToLocal " + _dfs_path + " " + _local_path,
            _verbose=_verbose, _log=_log)

        if result["code"] != 0:
            if not _keep_going:
                raise Exception("\n" + toString(result))

        return result

    def erasePathFromDFS(
        self,
        _path,
        _keep_going=False,
        _verbose=True,
        _log=sys.stdout
    ):
        result = runCommand(
            self.hadoop_cmd + " fs" +
            " -rm -r -f " + _path,
            _verbose=_verbose, _log=_log)

        if result["code"] != 0:
            if not _keep_going:
                raise Exception("\n" + toString(result))

        return result

    def getBasePath(
        self
    ):
        return self.hadoop_user_base_path

    def mkdirToDFS(
        self,
        _dfs_path,
        _keep_going=False,
        _verbose=True,
        _log=sys.stdout
    ):
        result = runCommand(
            self.hadoop_cmd + " fs" +
            " -mkdir -p " + _dfs_path,
            _verbose=_verbose, _log=_log)

        if result["code"] != 0:
            if not _keep_going:
                raise Exception("\n" + toString(result))

        return result

    def uploadDataToDFS(
        self,
        _local_path,
        _dfs_path,
        _keep_going=False,
        _verbose=True,
        _log=sys.stdout
    ):
        result = self.mkdirToDFS(os.path.dirname(_dfs_path))

        if result["code"] != 0:
            if not _keep_going:
                raise Exception("\n" + toString(result))

        result = runCommand(
            self.hadoop_cmd + " fs" +
            " -D dfs.blocksize=" + str(self.hadoop_blocksize) +
            " -copyFromLocal " + _local_path + " " + _dfs_path,
            _verbose=_verbose, _log=_log)

        if result["code"] != 0:
            if not _keep_going:
                raise Exception("\n" + toString(result))

        return result
