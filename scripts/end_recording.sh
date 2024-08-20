#!/bin/bash

# This script is used to stop the telemetry processes and collect results.

WORKLOAD=$1

machines=(
"h1" "h2" "h3" "h4" "h5" "h6" "h7" "h8" "h9" "h10" "h11" "h12" "h13"
"h14" "h15" "h16" "h17" "h18" "h19" "h20" "h21" "h22" "h23")
nservers=23

i=0
while [ $i != $nservers ]
do
    ssh "${machines[i]}.${EXP_NAME}.${PROJ_NAME}" "sudo ${SCRIPTS_DIR}/node_actions/end_recording.sh" &
    i=$(($i+1))
done

# Results are all written out, aggregate all data
sudo ./${SEEKWATCHER_DIR}/setup.py install

# aggregate blktrace output
cd ${RESULTS_DIR}/"$WORKLOAD"/output
seekwatcher -t ${RESULTS_DIR}/"$WORKLOAD"/blktrace_raw/h

# aggregate space results
sudo ./${SCRIPTS_DIR}/aggregate_space.py "$WORKLOAD"