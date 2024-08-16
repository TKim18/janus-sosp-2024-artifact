#!/bin/bash

# Hybrid lifetime transitions
NUM_CLIENTS=2
NUM_THREADS_PER_CLIENTS=2
NUM_FILES_PER_THREAD=20
FILE_SIZE=100663296 # 192 MBs
BASE_DIR="ec63cc"
TARGET_DIR="ec63cc"
FILE_PREFIX="hybrid" # this may change between runs
start_time=$(date +%s)

echo "[$(($(date +%s) - $start_time))] Size before writing: $(~/exec_scripts/check_capacities.sh)" >> out.txt # get filesizes

# Assume this script is run from h20 node with dfs-perf setup already
cd /proj/HeARTy/dfs-perf
cp conf/envs/dfs-perf-env-hybrid.sh conf/dfs-perf-env.sh # specify to write to cc directory

# Run write
before_time=$(date +%s)
./write.sh
after_time=$(date +%s)
cd -
echo "[$(($(date +%s) - $start_time))] Took $(($after_time - $before_time)) seconds to write files" >> out.txt

# Wait for data to be flushed
for ((it=0; it<5; it++))
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

echo "[$(($(date +%s) - $start_time))] Transitioning from hybrid to single-copy hybrid." >> out.txt
before_time=$(date +%s)
# Run ectransitioner to transition to single copy
file_names=""
for ((c=0; c<NUM_CLIENTS; c++))
do
	for ((t=0; t<NUM_THREADS_PER_CLIENTS; t++))
	do
		for ((f=0; f<NUM_FILES_PER_THREAD; f++))
		do
			if [ $f -eq 0 -a $c -eq 0 -a $t -eq 0 ]
			then
				file_names="/${BASE_DIR}/simple-read-write/${c}/${FILE_PREFIX}-${t}-${f}"
			else
				file_names="${file_names},/${BASE_DIR}/simple-read-write/${c}/${FILE_PREFIX}-${t}-${f}"
			fi
		done
	done
done
hdfs ectransitioner -fileName ${file_names} -codec RS -numDataUnits 2 -numParityUnits 1
after_time=$(date +%s)
echo "[$(($(date +%s) - $start_time))] Took $(($after_time - $before_time)) seconds to move to single copy" >> out.txt

# Wait for data to be transitioned
for ((it=0; it<10; it++))
do
	echo "[$(($(date +%s) - $start_time))] Size after moving to single copy: $(~/exec_scripts/check_capacities.sh)" >> out.txt
    sleep 5
done
sleep 120

# Clear cache
cd ~/exec_scripts/
./clear_cache.sh
cd -
sleep 30

echo "[$(($(date +%s) - $start_time))] Transitioning to EC from hybrid." >> out.txt
before_time=$(date +%s)
# Run ectransitioner to transition to just ec
file_names=""
for ((c=0; c<NUM_CLIENTS; c++))
do
	for ((t=0; t<NUM_THREADS_PER_CLIENTS; t++))
	do
		for ((f=0; f<NUM_FILES_PER_THREAD; f++))
		do
			if [ $f -eq 0 -a $c -eq 0 -a $t -eq 0 ]
			then
				file_names="/${BASE_DIR}/simple-read-write/${c}/${FILE_PREFIX}-${t}-${f}"
			else
				file_names="${file_names},/${BASE_DIR}/simple-read-write/${c}/${FILE_PREFIX}-${t}-${f}"
			fi
		done
	done
done
hdfs ectransitioner -fileName ${file_names} -codec CC -numDataUnits 6 -numParityUnits 3
after_time=$(date +%s)
echo "[$(($(date +%s) - $start_time))] Took $(($after_time - $before_time)) seconds to transition to EC" >> out.txt

# Wait for data to be transitioned
for ((it=0; it<10; it++))
do
	echo "[$(($(date +%s) - $start_time))] Size after transitioning to just EC: $(~/exec_scripts/check_capacities.sh)" >> out.txt
    sleep 5
done
sleep 120

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
				file_names="/${BASE_DIR}/simple-read-write/${c}/${FILE_PREFIX}-${t}-${f}"
			else
				file_names="${file_names},/${BASE_DIR}/simple-read-write/${c}/${FILE_PREFIX}-${t}-${f}"
			fi
		done
		hdfs ectransitioner -fileName ${file_names} -codec CC -numDataUnits 12 -numParityUnits 3
		echo "[$(($(date +%s) - $start_time))] Storage check: $(~/exec_scripts/check_capacities.sh)" >> out.txt
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




