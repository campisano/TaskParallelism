#!/bin/sh

mkdir -p bin;
rm -rf outputs;
rm -f task_parallelism.zip;
zip -q -r task_parallelism.zip task_parallelism;
spark-submit --master spark://sparkmaster:7077\
 --py-files task_parallelism.zip run.py\
 MySimpleTest tasks.csv bin inputs outputs\
 task_parallelism.dfs.DFSHadoop.DFSHadoopFactory\
 task_parallelism.exe.ExeSpark.ExeSparcFactory;

# End
