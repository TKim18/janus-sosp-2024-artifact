#!/bin/bash

ORIGINAL_POLICY="/ec21"
DESTINATION_POLICY="/ec41" # currently unused

# SETUP=create a bunch of random smaller files in HDFS
num_files=10
file_size=20971520
for ((i=0; i<=num_files; i++))
do
    # generate a random file
    tr -dc A-Za-z0-9 </dev/urandom | head -c ${file_size} > "random_${i}.txt"

    # write it to hdfs at the right directory
    hdfs dfs -put "random_${i}.txt" $ORIGINAL_POLICY

    # confirm it was written
    hdfs dfs -ls $ORIGINAL_POLICY

    # remove it from local server
    rm "random_${i}.txt"
done

sleep 60

# SCENARIO=transition files one by one
for ((i=0; i<=num_files; i++))
do
    # transition a file
    hdfs ectransitioner -fileName "${ORIGINAL_POLICY}/random_${i}.txt" -numDataUnits 4 -numParityUnits 1

    # pause
    sleep 3
done
