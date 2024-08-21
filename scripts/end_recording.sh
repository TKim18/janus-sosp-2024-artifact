#!/bin/bash

# This script is used to stop the telemetry processes. This cannot be run as sudo.

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