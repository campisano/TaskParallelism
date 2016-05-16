#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import os
import sys
from ..utils.os_utils import runCommand
from ..utils.string_utils import toString
from DistributedFileSistem import DistributedFileSistem
from DistributedFileSistem import DistributedFileSistemFactory


class DFSFermiGridFactory(DistributedFileSistemFactory):

    def __init__(
        self
    ):
        ####
        # Defining environment
        if not os.getenv("USER"):
            raise Exception("Can not find env variable 'USER'.")

        self.user = os.getenv("USER")

        ####
        # Define initial vars
        self.fermidfs_base_path = os.path.join("/pnfs/des/scratch", self.user)

    def create(
        self
    ):
        return DFSFermiGrid(self.fermidfs_base_path)


class DFSFermiGrid(DistributedFileSistem):

    def __init__(
        self,
        _fermidfs_base_path
    ):
        self.fermidfs_base_path = _fermidfs_base_path

        ####
        # Setup env script
        setupFail = False

        if not setupFail:
            result = runCommand(
                "/grid.cern.ch/util/cvmfs-uptodate"
                " /cvmfs/des.opensciencegrid.org",
                _verbose=True)

            if result["code"] != 0:
                setupFail = True

        if not setupFail:
            # sets up basic UPS and EUPS functionality,
            # and adds ifdh and jobsub to the PATH
            result = runCommand(
                "source"
                " /cvmfs/des.opensciencegrid.org/eeups/startupcachejob21i.sh",
                _verbose=True)

            if result["code"] != 0:
                setupFail = True

        if not setupFail:
            # set  up some actual software, which will all come from CVMFS
            result = runCommand(
                "setup Y2Nstack 1.0.6+18",
                _verbose=True)

            if result["code"] != 0:
                    setupFail = True

        if not setupFail:
            self.downloadDataCommand = "ifdh cp -r -D"
            self.erasePathCommand = "ifdh rm -r -D"
            self.mkdirCommand = "ifdh mkdir"
            self.uploadDataCommand = "ifdh cp -r -D"
        else:
            self.downloadDataCommand = "cp -a"
            self.erasePathCommand = "rm -rf"
            self.mkdirCommand = "mkdir"
            self.uploadDataCommand = "cp -a"

    def downloadDataFromDFS(
        self,
        _dfs_path,
        _local_path,
        _keep_going=False,
        _verbose=True,
        _log=sys.stdout
    ):
        result = runCommand(
            self.downloadDataCommand + " " + _dfs_path + " " + _local_path,
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
            self.erasePathCommand + " " + _path,
            _verbose=_verbose, _log=_log)

        if result["code"] != 0:
            if not _keep_going:
                raise Exception("\n" + toString(result))

        return result

    def getBasePath(
        self
    ):
        return self.fermidfs_base_path

    def mkdirToDFS(
        self,
        _dfs_path,
        _keep_going=False,
        _verbose=True,
        _log=sys.stdout
    ):
        result = runCommand(
            self.mkdirCommand + " " + _dfs_path,
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
            self.uploadDataCommand + " " + _local_path + " " + _dfs_path,
            _verbose=_verbose, _log=_log)

        if result["code"] != 0:
            if not _keep_going:
                raise Exception("\n" + toString(result))

        return result
