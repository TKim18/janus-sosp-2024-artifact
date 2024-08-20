#!/bin/bash

# This script will restart the hdfs cluster with no data

# stop cluster
ssh "h0.disks.HeARTy" "sudo $(pwd)/../hdfs/scripts/stop-dfs.sh"

# delete all data
machines=(
"h1" "h2" "h3" "h4" "h5" "h6" "h7" "h8" "h9" "h10" "h11" "h12" "h13"
"h14" "h15" "h16" "h17" "h18" "h19" "h20" "h21" "h22" "h23")
nservers=23

i=0
while [ $i != $nservers ]
do
    ssh "${machines[i]}.disks.HeARTy" "sudo $(pwd)/node_actions/wipe_node.sh" &
    i=$(($i+1))
done

# start cluster and add folders
ssh "h0.disks.HeARTy" "sudo $(pwd)/../hdfs/scripts/start-dfs.sh && $(pwd)/../hdfs/scripts/add-policies.sh"