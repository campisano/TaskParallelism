#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# task_parallelism.py v2.1
#

import argparse
import datetime
import os
import pprint
import re
import socket
import sys
import subprocess
from pyspark import SparkContext, SparkConf


class TaskParallelism:

    def __init__(
        self,
        _experiment_name,
        _tasks_csv_path,
        _binaries_path,
        _input_path,
        _output_path
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

        ####
        # Defining environment
        if not os.getenv("HADOOP_HOME"):
            raise Exception("Can not find env variable 'HADOOP_HOME'.")

        self.hadoop_cmd = os.path.join(
            os.getenv("HADOOP_HOME"), "bin", "hadoop")
        self.hadoop_blocksize = 4194304     # 4MB

        self.experiment_name = _experiment_name
        self.base_dfs_path = os.path.join(
            os.sep, self.getCleanPathName(self.experiment_name))

        self.tasks_csv_path = _tasks_csv_path
        self.tasks_csv_dfs_path = os.path.join(
            self.base_dfs_path, os.path.basename(_tasks_csv_path))
        self.binaries_path = _binaries_path
        self.binaries_dfs_path = os.path.join(
            self.base_dfs_path, os.path.basename(_binaries_path))
        self.input_path = _input_path
        self.input_dfs_path = os.path.join(
            self.base_dfs_path, os.path.basename(_input_path))
        self.output_path = _output_path
        self.output_dfs_path = os.path.join(
            self.base_dfs_path, os.path.basename(_output_path))

        ####
        # Cleanup data folder
        self.cleanupDFS(self.base_dfs_path)

        ####
        # Upload data to distributed file system
        self.uploadDataToDFS(self.tasks_csv_path, self.tasks_csv_dfs_path)
        self.uploadDataToDFS(self.binaries_path, self.binaries_dfs_path)
        self.uploadDataToDFS(self.input_path, self.input_dfs_path)

        ####
        # Run all tasks parallel
        self.runMaster()

        ####
        # Download data from distributed file system
        self.downloadDataFromDFS(self.output_dfs_path, self.output_path)

    def runMaster(self):

        sc = SparkContext(
            conf=SparkConf().setAppName(
                self.experiment_name).set(
                    "spark.app.id", self.experiment_name)
            # .set("spark.locality.wait", "0s")
        )

        # Get all rows
        csv_rows_RDD = sc.textFile(self.tasks_csv_dfs_path)

        # Get all words
        results_RDD = csv_rows_RDD.map(self.mapRunEachTask)

        # Group all results by key and sum their occurrencies
        reduced_results_RDD = results_RDD.reduceByKey(self.reduceTaskResults)

        # Collect the ouput
        outputs = reduced_results_RDD.collect()

        # Print the output
        for (path, output) in outputs:
            print(
                "%s:\n%s\n" % (
                    path.encode('ascii', 'ignore'),
                    output.encode('ascii', 'ignore')))

        sc.stop()

    def mapRunEachTask(self, _csv_row):

        ####
        # Get parameters
        cols = _csv_row.split(",")
        task_id = cols[0]
        command = cols[1]
        arguments = cols[2].split(" ")
        inputs = cols[3].split(" ")
        outputs = cols[4].split(" ")
        local_path = self.getCleanPathName(task_id)
        env_path = "./bin:" + os.environ["PATH"]

        ####
        # Setup Hadoop log
        home = os.environ["HOME"]
        hadoop_log_dir = os.path.join(home, "hadoop", "log")
        result = self.runCommand("mkdir -p " + hadoop_log_dir)
        os.environ["HADOOP_LOG_DIR"] = hadoop_log_dir

        ####
        # Prepare local paths
        result = self.runCommand(
            "rm -rf " + local_path,
            _verbose=True)
        result = self.runCommand(
            "mkdir -p " + local_path,
            _verbose=True)

        if result["code"] != 0:
            raise Exception("\n" + self.toString(result))

        log_file = open(os.path.join(local_path, "run.log"), "w")

        # grants upload of the log file
        try:
            self.mkdirToDFS(self.output_dfs_path, _log=log_file)

            ####
            # Print debug informations

            # Task
            log_file.write("\n==== Task: %s ====\n" % command)
            log_file.write("\t* arguments:\n")
            for arg in arguments:
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

            result = self.runCommand(
                "ls -l " + ".",
                _verbose=True, _log=log_file)

            log_file.write("\n==== Run dir content: ====\n")

            result = self.runCommand(
                "ls -l " + local_path,
                _verbose=True, _log=log_file)

            ####
            # Preparing
            log_file.write("\n==== Preparing: ====\n")

            # Download the bin data
            log_file.write("\t* Download the bin data:\n")
            self.downloadDataFromDFS(
                self.binaries_dfs_path, os.path.join(local_path, "bin"),
                _log=log_file)

            # Grant executable permission to bin/* files
            log_file.write("\t* Grant executable permission to bin/* files:\n")
            result = self.runCommand(
                "chmod -R 0755 " + os.path.join(local_path, "bin"),
                _verbose=True, _log=log_file)

            # Download the input data
            log_file.write("\t* Download the input data:\n")
            for input in inputs:
                self.downloadDataFromDFS(
                    os.path.join(self.input_dfs_path, input),
                    os.path.join(local_path, input),
                    _log=log_file)

            ####
            # Run the program
            log_file.write("\n==== Running: ====\n")
            result = self.runCommand(
                command + " " + " ".join(arguments),
                _work_dir=local_path,
                _env_path=env_path,
                _verbose=True, _log=log_file)

            log_file.write("\n==== Run dir content: ====\n")

            result = self.runCommand(
                "ls -l " + local_path,
                _verbose=True, _log=log_file)

            ####
            # Saving the results
            log_file.write("\n==== Saving the results: ====\n")

            # Upload the output data
            log_file.write("\t* Upload the output data:\n")

            for output in outputs:
                self.uploadDataToDFS(
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

            self.uploadDataToDFS(
                os.path.join(local_path, "run.log"),
                os.path.join(self.output_dfs_path, local_path, "run.log"),
                _keep_going=True)

        return (local_path, self.toString(result))

    def reduceTaskResults(self, _result_1, _result_2):
        return (
            _result_1 + "\nSTRANGE: REDUCED USED HERE!"
            " THIS MEAN THAT MORE THAN ONE LINE OF CSV ARE EQUALS."
            " ONE OF THEM WILL HAVE NO OUTPUT UPLOADED\n"
            + _result_2
        )

    def getCleanPathName(self, _path):
        return re.sub("[^0-9a-zA-Z]+", "_", _path)

    def cleanupDFS(self, _path, _keep_going=False):
        result = self.runCommand(
            self.hadoop_cmd + " fs" +
            " -rm -r -f " + _path,
            _verbose=True)

        if result["code"] != 0:
            if not _keep_going:
                raise Exception("\n" + self.toString(result))

        return result

    def mkdirToDFS(
        self, _dfs_path, _keep_going=False,
        _verbose=True, _log=sys.stdout
    ):
        result = self.runCommand(
            self.hadoop_cmd + " fs" +
            " -mkdir -p " + _dfs_path,
            _verbose=_verbose, _log=_log)

        if result["code"] != 0:
            if not _keep_going:
                raise Exception("\n" + self.toString(result))

        return result

    def uploadDataToDFS(
        self, _local_path, _dfs_path, _keep_going=False,
        _verbose=True, _log=sys.stdout
    ):
        result = self.mkdirToDFS(os.path.dirname(_dfs_path))

        if result["code"] != 0:
            if not _keep_going:
                raise Exception("\n" + self.toString(result))

        result = self.runCommand(
            self.hadoop_cmd + " fs" +
            " -D dfs.blocksize=" + str(self.hadoop_blocksize) +
            " -copyFromLocal " + _local_path + " " + _dfs_path,
            _verbose=_verbose, _log=_log)

        if result["code"] != 0:
            if not _keep_going:
                raise Exception("\n" + self.toString(result))

        return result

    def downloadDataFromDFS(
        self, _dfs_path, _local_path, _keep_going=False,
        _verbose=True, _log=sys.stdout
    ):
        result = self.runCommand(
            self.hadoop_cmd + " fs" +
            " -copyToLocal " + _dfs_path + " " + _local_path,
            _verbose=_verbose, _log=_log)

        if result["code"] != 0:
            if not _keep_going:
                raise Exception("\n" + self.toString(result))

        return result

    def toString(self, _an_object, _indent=2, _width=1):
        pp = pprint.PrettyPrinter(indent=_indent, width=_width)

        return pp.pformat(_an_object)

    def runCommand(
        self, _command, _work_dir=None, _env_path=None,
        _verbose=False, _log=sys.stdout
    ):

        if not _work_dir:
            _work_dir = os.getcwd()

        env_vars = os.environ

        if _env_path:
            env_vars["PATH"] = _env_path

        cmd = subprocess.Popen(
            _command, cwd=_work_dir, env=env_vars,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        return_data = cmd.communicate()
        result = {}
        result["out"] = return_data[0]
        result["err"] = return_data[1]
        result["code"] = cmd.returncode

        if _verbose:
            _log.write("Running: " + _command + '\n')
            if result["out"]:
                _log.write(" stdout:\n")
                _log.write(str(result["out"]) + "\n")
            if result["err"]:
                _log.write(" stderr:\n")
                _log.write(str(result["err"]) + "\n")
            if result["code"] != 0:
                _log.write(" retcode: " + str(result["code"]) + "\n")

        return result

    def chunksArguments(_l, _n):
        # from http://stackoverflow.com/a/312464
        """Yield successive n-sized chunks from _l."""
        for i in xrange(0, len(_l), _n):
            yield _l[i:i+_n]


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
    TaskParallelism(
        _experiment_name=args.experiment_name,
        _tasks_csv_path=args.tasks_csv_path,
        _binaries_path=args.binaries_path,
        _input_path=args.input_path,
        _output_path=args.output_path)
