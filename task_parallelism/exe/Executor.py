#!/usr/bin/env python
# -*- coding: utf-8 -*-
#


class ExecutorFactory:

    def create(
        self
    ):
        raise NotImplementedError()


class Executor:

    def runTask(
        self,
        _dfs_factory,
        _experiment_name,
        _tasks_csv_path,
        _base_dfs_path,
        _binaries_dfs_path,
        _input_dfs_path,
        _output_dfs_path
    ):
        raise NotImplementedError()
