#!/bin/bash

ORIGINAL_POLICY="/ec61"
DESTINATION_POLICY="/ec{$1}1"

num_files=0
file_size=1048576000  # 1 GB

# SCENARIO=transition files one by one
for ((i=0; i<=num_files; i++))
do
    hadoop distcp -skipcrccheck "hdfs://h0.disks.hearty.narwhal.pdl.cmu.edu:9000${ORIGINAL_POLICY}/random_${file_size}_${i}.txt" "hdfs://h0.disks.hearty.narwhal.pdl.cmu.edu:9000${DESTINATION_POLICY}"
    hdfs dfs -rm "${ORIGINAL_POLICY}/random_${file_size}_${i}.txt"

    # pause
    sleep 5
done
