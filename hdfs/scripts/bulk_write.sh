#!/bin/bash
# hard-coding these for simplicity, use params later
num_files=1
file_size=4096
#stripe_directory="/ec71"
start=2
end=15
for ((i=1; i<=num_files; i++))
do
  for ((j=${start}; j<=${end}; j++))
  do
    stripe_directory="/ec${j}1"
    hdfs dfs -put "/users/ttk2/symlinks/dare/scripts/experiments/sample_files/${file_size}mb_${i}.txt" ${stripe_directory}
  done
done