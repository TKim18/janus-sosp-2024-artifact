#!/bin/bash

# This script will start up the hdfs cluster from the Namenode at h0.
ssh "h0.disks.HeARTy" "sudo -o StrictHostKeyChecking=no $(pwd)/../hdfs/scripts/setup-cluster.sh" &
