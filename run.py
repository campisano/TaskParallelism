#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import argparse
from task_parallelism.WorkflowManager import WorkflowManager
from task_parallelism.dfs.DFSHadoop import DFSHadoopFactory
from task_parallelism.exe.ExeSpark import ExeSparcFactory


if __name__ == "__main__":

    ####
    # Defining program arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "experiment_name", type=str, help=(
            "Name of the current run experiment."))
    parser.add_argument(
        "tasks_csv_path", type=str, help=(
            "CSV file containing a list of task as"
            " 'command, arguments, inputs, outputs'"
            " to run parallel over all the nodes."))
    parser.add_argument(
        "binaries_path", type=str, nargs="?", help=(
            "Binary path of executables to deploy in every node."))
    parser.add_argument(
        "input_path", type=str, nargs="?", help=(
            "Input path to data needs for run all the tasks."))
    parser.add_argument(
        "output_path", type=str, help=(
            "Output path to use to deploy"
            " the data produced by all the completed tasks."))
    args = parser.parse_args()

    ####
    # Run the program
    WorkflowManager(
        _experiment_name=args.experiment_name,
        _tasks_csv_path=args.tasks_csv_path,
        _binaries_path=args.binaries_path,
        _input_path=args.input_path,
        _output_path=args.output_path,
        _dfsFactory=DFSHadoopFactory(),
        _exeFactory=ExeSparcFactory()
    )
