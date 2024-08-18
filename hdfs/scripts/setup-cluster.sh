#!/bin/bash

cd "/proj/HeARTy/ceridwen-sosp-2024-artifact/hdfs/scripts"

# build hdfs from source
bash install-hdfs.sh

# copy configs to dist configs
cp configs/* "/proj/HeARTy/ceridwen-sosp-2024-artifact/hdfs/hadoop-dist/target/hadoop-3.3.1/etc/hadoop/"

# start dfs
yes Y | bash start-dfs.sh

# add policies and create directories
bash add-policies.sh