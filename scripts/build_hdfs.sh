#!/bin/bash

ssh "h0.${EXP_NAME}.${PROJ_NAME}" -o StrictHostKeyChecking=no "source ~/.bashrc && cd ${HDFS_DIR}/scripts && bash ./install-hdfs.sh && sudo chmod -R 777 ${HDFS_DIR} && sudo chown -R evaluser:root ${HDFS_DIR}/hadoop-dist/target"