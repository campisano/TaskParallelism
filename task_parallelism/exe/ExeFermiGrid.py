#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import datetime
import os
import socket
from ..utils.os_utils import getCleanPathName
from ..utils.os_utils import getInstance
from ..utils.os_utils import runCommand
from ..utils.string_utils import toString
from Executor import Executor
from Executor import ExecutorFactory


class ExeFermiGridFactory(ExecutorFactory):

    def create(
        self
    ):
        return ExeFermiGrid()


class ExeFermiGrid(Executor):

    def mapRunEachTask(
        self,
        _csv_row
    ):
        dfs = self.dfs_factory.create()

        dfs.uploadDataToDFS(
            "task_parallelism.zip", self.base_dfs_path,
            _keep_going=True)

        run_single_task_filename = "run_single_task.sh"
        run_single_task_file = open(run_single_task_filename, "w")
        run_single_task_file.write(
            "#!/bin/bash" + "\n" +
            "\n" +
            "unzip task_parallelism.zip" + "\n" +
            "\n" +
            "python -c \"from task_parallelism.exe.ExeFermiGrid" +
            " import ExeFermiGrid; ExeFermiGrid().runTask('" +
            _csv_row.strip() + "', '" +
            self.binaries_dfs_path + "', '" +
            self.input_dfs_path + "', '" +
            self.output_dfs_path + "', '" +
            str(self.dfs_factory.__class__) +
            "')\""
        )
        run_single_task_file.close()

        # submit with the -f option
        # from
        # https://cdcvs.fnal.gov/redmine/projects/jobsub/wiki/Jobsub_submit

    def reduceTaskResults(
        self,
        _jobs,
    ):
        pass

    def runAllTasks(
        self,
        _dfs_factory,
        _experiment_name,
        _tasks_csv_path,
        _base_dfs_path,
        _binaries_dfs_path,
        _input_dfs_path,
        _output_dfs_path
    ):
        self.dfs_factory = _dfs_factory
        self.base_dfs_path = _base_dfs_path
        self.binaries_dfs_path = _binaries_dfs_path
        self.input_dfs_path = _input_dfs_path
        self.output_dfs_path = _output_dfs_path

        # Get all rows
        csv_rows = open(_tasks_csv_path, "r").readlines()

        jobs = list()

        # Get all results

        for row in csv_rows:
            jobs.append(self.mapRunEachTask(row))

        # Group all results by key and reduce their occurrencies
        # outputs = self.reduceTaskResults(jobs)

        # Print the output
        # for (path, output) in outputs:
        #     print(
        #         "%s:\n%s\n" % (
        #             path.encode('ascii', 'ignore'),
        #             output.encode('ascii', 'ignore')))

    def runTask(
        self,
        _csv_row,
        _binaries_dfs_path,
        _input_dfs_path,
        _output_dfs_path,
        _dfs_factory_module_class_name
    ):
        dfs_factory = getInstance(_dfs_factory_module_class_name)
        dfs = dfs_factory.create()

        ####
        # Get parameters
        cols = _csv_row.split(",")
        task_id = cols[0]
        command = cols[1].split(" ")
        inputs = cols[2].split(" ")
        outputs = cols[3].split(" ")

        local_path = getCleanPathName(task_id)
        env_path = "./bin:" + os.environ["PATH"]

        ####
        # Prepare local paths
        result = runCommand(
            "rm -rf " + local_path,
            _verbose=True)

        result = runCommand(
            "mkdir -p " + local_path,
            _verbose=True)

        if result["code"] != 0:
            raise Exception("\n" + toString(result))

        log_file = open(os.path.join(local_path, "run.log"), "w")

        # grants upload of the log file
        try:
            dfs.mkdirToDFS(_output_dfs_path, _log=log_file)

            ####
            # Print debug informations

            # Task
            log_file.write("\n==== Task: %s ====\n" % command[0])
            log_file.write("\t* arguments:\n")
            for arg in command[1:]:
                log_file.write("\t\t%s\n" % arg)
            log_file.write("\t* inputs:\n")
            for input in inputs:
                log_file.write("\t\t%s\n" % input)
            log_file.write("\t* outputs:\n")
            for output in outputs:
                log_file.write("\t\t%s\n" % output)

            # Env
            log_file.write("\n==== Env: ====\n")
            log_file.write("\tcurrent hostname:\t%s\n" % socket.gethostname())
            log_file.write("\tcurrent time:\t%s\n" % (
                datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            log_file.write("\tcurrent work dir:\t%s\n" % os.getcwd())
            log_file.write("\trun work dir:\t%s\n" % (
                os.path.join(os.getcwd(), local_path)))
            log_file.write("\tcurrent env path:\t%s\n" % os.environ["PATH"])
            log_file.write("\trun env path:\t%s\n" % env_path)

            log_file.write("\n==== Current dir content: ====\n")

            result = runCommand(
                "ls -l " + ".",
                _verbose=True, _log=log_file)

            log_file.write("\n==== Run dir content: ====\n")

            result = runCommand(
                "ls -l " + local_path,
                _verbose=True, _log=log_file)

            ####
            # Preparing
            log_file.write("\n==== Preparing: ====\n")

            # Download the bin data
            log_file.write("\t* Download the bin data:\n")
            dfs.downloadDataFromDFS(
                _binaries_dfs_path, os.path.join(local_path, "bin"),
                _log=log_file)

            # Grant executable permission to bin/* files
            log_file.write("\t* Grant executable permission to bin/* files:\n")
            result = runCommand(
                "chmod -R 0755 " + os.path.join(local_path, "bin"),
                _verbose=True, _log=log_file)

            # Download the input data
            log_file.write("\t* Download the input data:\n")
            for input in inputs:
                dfs.downloadDataFromDFS(
                    os.path.join(_input_dfs_path, input),
                    os.path.join(local_path, input),
                    _log=log_file)

            ####
            # Run the program
            log_file.write("\n==== Running: ====\n")
            result = runCommand(
                " ".join(command),
                _work_dir=local_path,
                _env_path=env_path,
                _verbose=True, _log=log_file)

            log_file.write("\n==== Run dir content: ====\n")

            result = runCommand(
                "ls -l " + local_path,
                _verbose=True, _log=log_file)

            ####
            # Saving the results
            log_file.write("\n==== Saving the results: ====\n")

            # Upload the output data
            log_file.write("\t* Upload the output data:\n")

            for output in outputs:
                dfs.uploadDataToDFS(
                    os.path.join(local_path, output),
                    os.path.join(_output_dfs_path, local_path, output),
                    _keep_going=True, _log=log_file)
        except:
            log_file.write("\n\n\n#### Error ####\n")
            raise

        finally:
            # Upload log file
            log_file.write("\t* Upload log file.\n")
            log_file.close()   # Important! we need to close it before!

            dfs.uploadDataToDFS(
                os.path.join(local_path, "run.log"),
                os.path.join(_output_dfs_path, local_path, "run.log"),
                _keep_going=True)

        return (local_path, toString(result))
