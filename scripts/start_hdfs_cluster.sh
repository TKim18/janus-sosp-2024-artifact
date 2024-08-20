#!/bin/bash

# This script will start up the hdfs cluster from the Namenode at h0.
ssh "h0.disks.sosp24eval" "sudo -o StrictHostKeyChecking=no /proj/sosp24eval/janus-sosp-2024-artifact/hdfs/scripts/setup-cluster.sh" &
