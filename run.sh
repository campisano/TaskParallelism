#!/bin/sh

mkdir -p bin;
rm -rf outputs;
spark-submit --master spark://sparkmaster:7077 ../task_parallelism.py MySimpleTest tasks.csv bin inputs outputs

# End
