#!/bin/bash

# This script will restart the hdfs cluster with no data

# stop cluster, can't be run as root otherwise profile will be gone
ssh "h0.${EXP_NAME}.${PROJ_NAME}" -o StrictHostKeyChecking=no "source ~/.bashrc && bash ${HDFS_DIR}/scripts/stop-dfs.sh"

# delete all data
machines=(
"h1" "h2" "h3" "h4" "h5" "h6" "h7" "h8" "h9" "h10" "h11" "h12" "h13"
"h14" "h15" "h16" "h17" "h18" "h19" "h20" "h21" "h22" "h23")
nservers=23

i=0
while [ $i != $nservers ]
do
    ssh "${machines[i]}.${EXP_NAME}.${PROJ_NAME}" -o StrictHostKeyChecking=no "sudo ${SCRIPTS_DIR}/node_actions/wipe_node.sh" &
    i=$(($i+1))
done

# start cluster and add folders
ssh "h0.${EXP_NAME}.${PROJ_NAME}" -o StrictHostKeyChecking=no "source ~/.bashrc && bash ${HDFS_DIR}/scripts/start-dfs.sh && bash ${HDFS_DIR}/scripts/add-policies.sh"