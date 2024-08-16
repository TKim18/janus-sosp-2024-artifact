#!/bin/bash

# Hybrid lifetime transitions
NUM_CLIENTS=2
NUM_THREADS_PER_CLIENTS=2
NUM_FILES_PER_THREAD=20
FILE_SIZE=100663296 # 96 MBs
BASE_DIR="rr3"
TARGET_DIR="ec63rs"
FILE_PREFIX="nonhybrid" # the file name is hybrid even for replicated files
start_time=$(date +%s)

echo "[$(($(date +%s) - $start_time))] Size before writing: $(~/exec_scripts/check_capacities.sh)" >> out.txt # get filesizes

# Assume this script is run from h20 node with dfs-perf setup already
cd /proj/HeARTy/dfs-perf
cp conf/envs/dfs-perf-env-replicated.sh conf/dfs-perf-env.sh # specify to write to cc directory

# Run write
before_time=$(date +%s)
./write.sh 
after_time=$(date +%s)
cd -
echo "[$(($(date +%s) - $start_time))] Took $(($after_time - $before_time)) seconds to write files" >> out.txt

# Wait for data to be flushed
for ((it=0; it<10; it++))
do
	echo "[$(($(date +%s) - $start_time))] Size after writing: $(~/exec_scripts/check_capacities.sh)" >> out.txt # get filesizes
	sleep 30
done

# Clear cache
cd ~/exec_scripts/
./clear_cache.sh
cd -
sleep 30

#####--------- FOR ALL TRANSITIONS ---------#####
# c = client id; t = thread id; f = file id

echo "[$(($(date +%s) - $start_time))] Transitioning from replicated to erasure-coded." >> out.txt
before_time=$(date +%s)
cd /proj/HeARTy/dfs-perf
cp conf/envs/dfs-perf-env-ec.sh conf/dfs-perf-env.sh # specify to write to cc directory
./write.sh
cd -

# Run distcp to transition from replicated to erasure-coded
for ((c=0; c<NUM_CLIENTS; c++))
do
	for ((t=0; t<NUM_THREADS_PER_CLIENTS; t++))
	do
		for ((f=0; f<NUM_FILES_PER_THREAD; f++))
		do
			# hadoop distcp -skipcrccheck "hdfs://h0-dfge:9000/${BASE_DIR}/simple-read-write/${c}/${FILE_PREFIX}-${t}-${f}" "hdfs://h0-dfge:9000/${TARGET_DIR}/simple-read-write/${c}/${FILE_PREFIX}-${t}-${f}"
			hdfs dfs -rm "/${BASE_DIR}/simple-read-write/${c}/${FILE_PREFIX}-${t}-${f}"
		done
	done
done
after_time=$(date +%s)
echo "[$(($(date +%s) - $start_time))] Took $(($after_time - $before_time)) seconds to transition to EC" >> out.txt

# Wait for data to be transitioned
for ((it=0; it<5; it++))
do
	echo "[$(($(date +%s) - $start_time))] Size after transitioning to just EC: $(~/exec_scripts/check_capacities.sh)" >> out.txt
    sleep 30
done

# Clear cache
cd ~/exec_scripts/
./clear_cache.sh
cd -
sleep 30

echo "[$(($(date +%s) - $start_time))] Transitioning to wider EC (12-wide)." >> out.txt
before_time=$(date +%s)
# Run ectransitioner to transition to wider ec
for ((c=0; c<NUM_CLIENTS; c++))
do
	for ((t=0; t<NUM_THREADS_PER_CLIENTS; t++))
	do
		file_names=""
		for ((f=0; f<NUM_FILES_PER_THREAD; f++))
		do
			if [[ $f -eq 0 ]]
			then
				file_names="/${TARGET_DIR}/simple-read-write/${c}/${FILE_PREFIX}-${t}-${f}"
			else
				file_names="${file_names},/${TARGET_DIR}/simple-read-write/${c}/${FILE_PREFIX}-${t}-${f}"
			fi
		done
		hdfs ectransitioner -fileName ${file_names} -codec RS -numDataUnits 12 -numParityUnits 3
		sleep 10
	done
done
after_time=$(date +%s)
echo "[$(($(date +%s) - $start_time))] Took $(($after_time - $before_time)) seconds to transition to wider EC" >> out.txt

for ((it=0; it<5; it++))
do
	echo "[$(($(date +%s) - $start_time))] Size after transitioning to wider EC: $(~/exec_scripts/check_capacities.sh)" >> out.txt
    sleep 30
done





