#!/bin/bash

ORIGINAL_POLICY="/ec21"
DESTINATION_POLICY="/ec41" # currently unused

# SETUP=create a bunch of random files in HDFS
num_files=19
file_size=209715200
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

sleep 120

# SCENARIO=transition files all at once
file_names=""
for ((i=0; i<=num_files; i++))
do
    if [[ $i -eq 0 ]]
    then
      file_names="${ORIGINAL_POLICY}/random_${i}.txt"
    else
      file_names="${file_names},${ORIGINAL_POLICY}/random_${i}.txt"
    fi
done

hdfs ectransitioner -fileName "${file_names}" -numDataUnits 4 -numParityUnits 1

sleep 120

# DESTROY=delete all files from hdfs, reset state
for ((i=0; i<=num_files; i++))
do
    # remove it from hdfs at the right directory
    hdfs dfs -rm "${ORIGINAL_POLICY}/random_${i}.txt"
done
