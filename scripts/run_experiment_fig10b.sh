#!/bin/bash

# This script can be run for both baseline and Janus.
# Pass in either as a string to the first parameter.
# In this script since we're already on node h24, we have to
# 1. make sure the right properties are in and code set (dfs-perf can be in lifetime mode or vanilla mode)
# - for this, potentially just copy dfs-perf since it's two code-bases
# 2. make sure the config is set properly
# 3. make sure to build dfs perf from source
# 3. run write from node

# call restarts with replication set to 3
sed -i 's/<value>2</value>/<value>3</value>/g' ${HDFS_DIR}/scripts/configs/hdfs-site.xml
bash ${SCRIPTS_DIR}/restart_hdfs_cluster.sh

# first run baseline results
bash ${SCRIPTS_DIR}/begin_recording.sh baseline
sleep 10

cd ${DFS_PERF_DIR}

# set config
sed -i 's/ec53cc/rr3/g' conf/dfs-perf-env.sh
sed -i 's/"dfs.replication", "2"/"dfs.replication", "3"/g' src/main/java/pasalab/dfs/perf/fs/PerfFileSystemHdfs.java

# build the project
bash install-dfs-perf.sh

# execute the workload
./execute.sh

sleep 10
bash ${SCRIPTS_DIR}/end_recording.sh
sudo ${SCRIPTS_DIR}/collect_results.sh baseline

# then run janus results
sed -i 's/<value>3</value>/<value>2</value>/g' ${HDFS_DIR}/scripts/configs/hdfs-site.xml
bash ${SCRIPTS_DIR}/restart_hdfs_cluster.sh

sleep 10

bash ${SCRIPTS_DIR}/begin_recording.sh janus
sleep 10

cd ${DFS_PERF_DIR}

# set config
sed -i 's/rr3/ec53cc/g' conf/dfs-perf-env.sh
sed -i 's/"dfs.replication", "3"/"dfs.replication", "2"/g' src/main/java/pasalab/dfs/perf/fs/PerfFileSystemHdfs.java

# build the project
bash install-dfs-perf.sh

# execute the workload
./execute.sh
cd -

sleep 10
bash ${SCRIPTS_DIR}/end_recording.sh
sudo ${SCRIPTS_DIR}/collect_results.sh janus