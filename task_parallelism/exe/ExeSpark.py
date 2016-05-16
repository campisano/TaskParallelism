#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import datetime
import os
import socket
from ..utils.os_utils import getCleanPathName
from ..utils.os_utils import runCommand
from ..utils.string_utils import toString
from pyspark import SparkContext, SparkConf
from Executor import Executor
from Executor import ExecutorFactory


class ExeSparcFactory(ExecutorFactory):

    def create(
        self
    ):
        return ExeSparc()


class ExeSparc(Executor):

    def mapRunEachTask(
        self,
        _csv_row
    ):
        dfs = self.dfs_factory.create()

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
            dfs.mkdirToDFS(self.output_dfs_path, _log=log_file)

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
                self.binaries_dfs_path, os.path.join(local_path, "bin"),
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
                    os.path.join(self.input_dfs_path, input),
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
                    os.path.join(self.output_dfs_path, local_path, output),
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
                os.path.join(self.output_dfs_path, local_path, "run.log"),
                _keep_going=True)

        return (local_path, toString(result))

    def reduceTaskResults(
        self,
        _result_1,
        _result_2
    ):
        return (
            _result_1 + "\nSTRANGE: REDUCED USED HERE!"
            " THIS MEAN THAT MORE THAN ONE LINE OF CSV ARE EQUALS."
            " ONE OF THEM WILL HAVE NO OUTPUT UPLOADED\n"
            + _result_2
        )

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

        ####
        # Upload CSV data to distributed file system
        tasks_csv_dfs_path = os.path.join(
            _base_dfs_path, os.path.basename(_tasks_csv_path))
        dfs = _dfs_factory.create()
        dfs.uploadDataToDFS(_tasks_csv_path, tasks_csv_dfs_path)

        ####
        # Create spark context
        sc = SparkContext(
            conf=SparkConf().setAppName(
                _experiment_name).set(
                    "spark.app.id", _experiment_name)
            # .set("spark.locality.wait", "0s")
        )

        # Get all rows
        csv_rows_RDD = sc.textFile(tasks_csv_dfs_path)

        # Get all results
        results_RDD = csv_rows_RDD.map(self.mapRunEachTask)

        # Group all results by key and reduce their occurrencies
        reduced_results_RDD = results_RDD.reduceByKey(self.reduceTaskResults)

        # Collect the ouput
        outputs = reduced_results_RDD.collect()

        # Print the output
        for (path, output) in outputs:
            print(
                "%s:\n%s\n" % (
                    path.encode('ascii', 'ignore'),
                    output.encode('ascii', 'ignore')))

        ####
        # finalize spark context
        sc.stop()
