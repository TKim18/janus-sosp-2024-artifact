#!/bin/bash

# This script syncs and clears the buffer cache on all Datanodes.

machines=(
"h1" "h2" "h3" "h4" "h5" "h6" "h7" "h8" "h9" "h10" "h11" "h12" "h13"
"h14" "h15" "h16" "h17" "h18" "h19" "h20" "h21" "h22" "h23")
nservers=23

i=0
while [ $i != $nservers ]
do
    ssh "${machines[i]}.disks.HeARTy" -o StrictHostKeyChecking=no "sudo /proj/HeARTy/ceridwen-sosp-2024-artifact/scripts/node_actions/clear_cache.sh" >> out 2>> out &
    echo "success"
    i=$(($i+1))
done