#!/bin/bash

# This script will start up the hdfs cluster from the Namenode at h0.

# define absolute path
workspace_dir="/proj/HeARTy/ceridwen-sosp-2024-artifact"

# move to hdfs scripts dir
cd "${workspace_dir}/hdfs/scripts"

# build hdfs from source
bash install-hdfs.sh

# copy configs to dist configs
cp configs/* "${workspace_dir}/hdfs/hadoop-dist/target/hadoop-3.3.1/etc/hadoop/"

# start
yes Y | bash start-dfs.sh

# add policies
bash add-policies.sh