#!/bin/bash

ORIGINAL_POLICY="/ec61"

# SETUP=create a bunch of random files in HDFS
num_files=0
file_size=1048576000
# DESTROY=delete all files from hdfs, reset state
for ((i=0; i<=num_files; i++))
do
    # remove it from hdfs at the right directory
    hdfs dfs -rm "${ORIGINAL_POLICY}/random_${file_size}_${i}.txt"
done