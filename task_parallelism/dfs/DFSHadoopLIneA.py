#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import os
from DFSHadoop import DFSHadoop
from DFSHadoop import DFSHadoopFactory


class DFSHadoopLIneAFactory(DFSHadoopFactory):

    def __init__(
        self
    ):
        # TODO [CMP] gambi!
        if "PATH" not in os.environ:
            os.environ["PATH"] = "/bin:/usr/bin"

        os.environ["USER"] = "riccardo.campisano"
        os.environ["HOME"] = "/home/riccardo.campisano"
        os.environ["JAVA_HOME"] = "/tawala/packages/jdk-1.7u25-1"

        if os.path.isdir("/home/riccardo.campisano/hadoop-install/hadoop-2.6.0"):
            os.environ["HADOOP_HOME"] = (
                "/home/riccardo.campisano/hadoop-install/hadoop-2.6.0")

        if os.path.isdir("/hadoop-install/hadoop-2.6.0"):
            os.environ["HADOOP_HOME"] = (
                "/hadoop-install/hadoop-2.6.0")

        ####
        # Defining environment
        if not os.getenv("USER"):
            raise Exception("Can not find env variable 'USER'.")

        self.user = os.getenv("USER")

        self.hadoop_home = os.getenv("HADOOP_HOME")

        ####
        # Define initial vars
        self.hadoop_user_base_path = os.path.join("/user", self.user)
        self.hadoop_cmd = os.path.join(self.hadoop_home, "bin", "hadoop")
        self.hadoop_blocksize = 4194304     # 4MB

    def create(
        self
    ):
        return DFSHadoopLIneA(
            self.hadoop_user_base_path,
            self.hadoop_cmd,
            self.hadoop_blocksize)


class DFSHadoopLIneA(DFSHadoop):

    def __init__(
        self,
        _hadoop_user_base_path,
        _hadoop_cmd,
        _hadoop_blocksize
    ):
        # TODO [CMP] gambi!
        if "PATH" not in os.environ:
            os.environ["PATH"] = "/bin:/usr/bin"

        os.environ["USER"] = "riccardo.campisano"
        os.environ["HOME"] = "/home/riccardo.campisano"
        os.environ["JAVA_HOME"] = "/tawala/packages/jdk-1.7u25-1"

        if os.path.isdir("/home/riccardo.campisano/hadoop-install/hadoop-2.6.0"):
            os.environ["HADOOP_HOME"] = (
                "/home/riccardo.campisano/hadoop-install/hadoop-2.6.0")

        if os.path.isdir("/hadoop-install/hadoop-2.6.0"):
            os.environ["HADOOP_HOME"] = (
                "/hadoop-install/hadoop-2.6.0")

        DFSHadoop.__init__(
            self,
            _hadoop_user_base_path,
            _hadoop_cmd,
            _hadoop_blocksize
            )
