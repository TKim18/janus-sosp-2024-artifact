#!/bin/bash

ssh "h0.${EXP_NAME}.${PROJ_NAME}" -o StrictHostKeyChecking=no "source ~/.bashrc && bash ${HDFS_DIR}/scripts/install-hdfs.sh"