#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import sys


class DistributedFileSistemFactory:

    def create(
        self
    ):
        raise NotImplementedError()


class DistributedFileSistem:

    def cleanupDFS(
        self,
        _path,
        _keep_going=False
    ):
        raise NotImplementedError()

    def getBasePath(
        self
    ):
        raise NotImplementedError()

    def mkdirToDFS(
        self,
        _dfs_path,
        _keep_going=False,
        _verbose=True,
        _log=sys.stdout
    ):
        raise NotImplementedError()

    def uploadDataToDFS(
        self,
        _local_path,
        _dfs_path,
        _keep_going=False,
        _verbose=True,
        _log=sys.stdout
    ):
        raise NotImplementedError()

    def downloadDataFromDFS(
        self,
        _dfs_path,
        _local_path,
        _keep_going=False,
        _verbose=True,
        _log=sys.stdout
    ):
        raise NotImplementedError()
