#!/bin/bash

# This script will start up the hdfs cluster from the Namenode at h0.
ssh "h0.${EXP_NAME}.${PROJ_NAME}" -o StrictHostKeyChecking=no  "source ~/.bashrc && bash ${HDFS_DIR}/scripts/setup-cluster.sh"
