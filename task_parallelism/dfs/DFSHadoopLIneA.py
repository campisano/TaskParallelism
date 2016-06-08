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
        ####
        # TODO [CMP] gambi!
        if "PATH" not in os.environ:
            os.environ["PATH"] = "/bin:/usr/bin"

        os.environ["USER"] = "riccardo.campisano"
        os.environ["HOME"] = "/home/riccardo.campisano"
        os.environ["JAVA_HOME"] = "/tawala/packages/jdk-1.7u25-1"

        if os.path.isdir(
            "/home/riccardo.campisano/hadoop-install/hadoop-2.6.0"
        ):
            os.environ["HADOOP_HOME"] = (
                "/home/riccardo.campisano/hadoop-install/hadoop-2.6.0")

        if os.path.isdir("/hadoop-install/hadoop-2.6.0"):
            os.environ["HADOOP_HOME"] = (
                "/hadoop-install/hadoop-2.6.0")

        DFSHadoopFactory.__init__(self)

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
        ####
        # TODO [CMP] gambi!
        if "PATH" not in os.environ:
            os.environ["PATH"] = "/bin:/usr/bin"

        os.environ["USER"] = "riccardo.campisano"
        os.environ["HOME"] = "/home/riccardo.campisano"
        os.environ["JAVA_HOME"] = "/tawala/packages/jdk-1.7u25-1"

        if os.path.isdir(
            "/home/riccardo.campisano/hadoop-install/hadoop-2.6.0"
        ):
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
