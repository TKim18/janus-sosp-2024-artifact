#!/bin/bash

cd "/proj/HeARTy/ceridwen-sosp-2024-artifact/hdfs/scripts"

# build hdfs from source
bash install-hdfs.sh

# copy configs to dist configs - step is being done in start-dfs.sh
# cp configs/* "/proj/HeARTy/ceridwen-sosp-2024-artifact/hdfs/hadoop-dist/target/hadoop-3.3.1/etc/hadoop/"

# start dfs
bash start-dfs.sh

# add policies and create directories
bash add-policies.sh
