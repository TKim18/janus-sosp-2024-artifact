#!/bin/bash

ORIGINAL_POLICY="/ec61"

# SETUP=create a bunch of random files in HDFS
num_files=0
file_size=1048576000  # 1 GB
#for ((i=0; i<=num_files; i++))
#do
#    # write it to hdfs at the right directory
#    hdfs dfs -put "sample_files/random_${file_size}_${i}.txt" $ORIGINAL_POLICY
#
#    # confirm it was written
#    hdfs dfs -ls $ORIGINAL_POLICY
#done
#
#sleep 60

# SCENARIO=transition files one by one
for ((i=0; i<=num_files; i++))
do
    # transition a file
    hdfs ectransitioner -fileName "${ORIGINAL_POLICY}/random_${file_size}_${i}.txt" -codec XOR -numDataUnits $1 -numParityUnits 1

    # pause
    sleep 5
done

#sleep 60
#
## DESTROY=delete all files from hdfs, reset state
#for ((i=0; i<=num_files; i++))
#do
#    # remove it from hdfs at the right directory
#    hdfs dfs -rm "${ORIGINAL_POLICY}/random_${i}.txt"
#done
