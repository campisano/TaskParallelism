#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import os
import re
import sys
import subprocess


def getCleanPathName(
    _path
):
    return re.sub("[^0-9a-zA-Z]+", "_", _path)


def getInstance(
    _module_class_name
):
    module_name, class_name = _module_class_name.rsplit(".", 1)
    my_module = __import__(module_name, fromlist=[class_name])
    my_class = getattr(my_module, class_name)
    instance = my_class()

    return instance


def runCommand(
    _command,
    _work_dir=None,
    _env_path=None,
    _verbose=False,
    _log=sys.stdout
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
