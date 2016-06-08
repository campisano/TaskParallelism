#!/bin/sh

rm -rf condor;
rm -rf outputs;
python run.py\
 MySimpleTest tasks.csv bin inputs outputs\
 task_parallelism.dfs.DFSHadoopLIneA.DFSHadoopLIneAFactory\
 task_parallelism.exe.ExeHTCondor.ExeHTCondorFactory;

# End
