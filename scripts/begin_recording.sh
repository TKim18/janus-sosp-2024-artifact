#!/bin/bash

# This script is used to prepare and start the telemetry that we use to collect data.
# This script can be called in other experiment scripts with end_recording.
# Specifically, the two things we'll collect during this artifact evaluation is
# 1. storage space used (df -k)
# 2. disk bandwidth used (blktrace+blkparse+seekwatcher)
# In the paper submission's evaluation, we also include network metrics but due to the complexities
# of getting Ganglia set up and getting access to the results, we omit those metrics.
# After running this script, you must run end_recording to stop the telemetry and collect the data.

WORKLOAD=$1

# re-initialize results directory
rm -rf results/"$WORKLOAD"/blktrace_raw
rm -rf results/"$WORKLOAD"/output
rm -rf results/"$WORKLOAD"/space
mkdir -p results/"$WORKLOAD"/blktrace_raw
mkdir -p results/"$WORKLOAD"/output
mkdir -p results/"$WORKLOAD"/space

machines=(
"h1" "h2" "h3" "h4" "h5" "h6" "h7" "h8" "h9" "h10" "h11" "h12" "h13"
"h14" "h15" "h16" "h17" "h18" "h19" "h20" "h21" "h22" "h23")
nservers=23

i=0
while [ $i != $nservers ]
do
    ssh "${machines[i]}.disks.HeARTy" "sudo $(pwd)/node_actions/begin_recording.sh $(pwd)/results/${WORKLOAD}" &
    i=$(($i+1))
done