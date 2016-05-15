#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import os
from utils.os_utils import getCleanPathName


class WorkflowManager:

    def __init__(
        self,
        _experiment_name,
        _tasks_csv_path,
        _binaries_path,
        _input_path,
        _output_path,
        _dfsFactory,
        _exeFactory
    ):
        ####
        # Test arguments
        if not os.path.isfile(_tasks_csv_path):
            raise Exception(
                "'tasks_csv_path': '%s' must be a local CSV archive."
                % _tasks_csv_path)

        if not os.path.isdir(_binaries_path):
            raise Exception(
                "'binaries_path': '%s' must be a local directory."
                % _binaries_path)

        if not os.path.isdir(_input_path):
            raise Exception(
                "'input_path': '%s' must be a local directory."
                % _input_path)

        if os.path.exists(_output_path):
            raise Exception(
                "'output_path': '%s' must not exist."
                % _output_path)

        base_dfs_path = os.path.join(
            os.sep, getCleanPathName(_experiment_name))

        tasks_csv_dfs_path = os.path.join(
            base_dfs_path, os.path.basename(_tasks_csv_path))
        binaries_dfs_path = os.path.join(
            base_dfs_path, os.path.basename(_binaries_path))
        input_dfs_path = os.path.join(
            base_dfs_path, os.path.basename(_input_path))
        output_dfs_path = os.path.join(
            base_dfs_path, os.path.basename(_output_path))

        dfs = _dfsFactory.create()

        ####
        # Cleanup data folder
        dfs.cleanupDFS(base_dfs_path)

        ####
        # Upload data to distributed file system
        dfs.uploadDataToDFS(_tasks_csv_path, tasks_csv_dfs_path)
        dfs.uploadDataToDFS(_binaries_path, binaries_dfs_path)
        dfs.uploadDataToDFS(_input_path, input_dfs_path)

        exe = _exeFactory.create()

        ####
        # Run all tasks parallel
        exe.runAllTasks(
            _dfsFactory=_dfsFactory,
            _experiment_name=_experiment_name,
            _tasks_csv_dfs_path=tasks_csv_dfs_path,
            _binaries_dfs_path=binaries_dfs_path,
            _input_dfs_path=input_dfs_path,
            _output_dfs_path=output_dfs_path
        )

        ####
        # Download data from distributed file system
        dfs.downloadDataFromDFS(output_dfs_path, _output_path)
