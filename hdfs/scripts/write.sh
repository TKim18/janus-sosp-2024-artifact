#!/bin/bash
# hard-coding these for simplicity, use params later
num_files=1
file_size=192
stripe_directory="/ec83"
for ((i=1; i<=num_files; i++))
do
  #time hdfs dfs -put "/users/ttk2/symlinks/dare/scripts/experiments/sample_files/${file_size}mb_${i}.txt" ${stripe_directory}
  #time hdfs dfs -put "/users/ttk2/symlinks/dare/scripts/experiments/sample_files/${file_size}mb_${i}.txt" "/ec33"
  time hdfs dfs -put "/users/ttk2/symlinks/dare/scripts/experiments/sample_files/${file_size}mb_${i}.txt" "/ec63"
  #time hdfs dfs -put "/users/ttk2/symlinks/dare/scripts/experiments/sample_files/${file_size}mb_${i}.txt" "/ec83"
  #time hdfs dfs -put "/users/ttk2/symlinks/dare/scripts/experiments/sample_files/${file_size}mb_${i}.txt" "/ec123"
done
