#!/bin/bash

ssh "h0.${EXP_NAME}.${PROJ_NAME}" -o StrictHostKeyChecking=no "source ~/.bashrc && cd ${HDFS_DIR}/scripts && bash ./install-hdfs.sh"