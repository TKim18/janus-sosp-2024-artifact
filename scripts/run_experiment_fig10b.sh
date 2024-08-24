#!/bin/bash

# call restarts with replication set to 3
sed -i 's/<value>2</<value>3</g' ${HDFS_DIR}/scripts/configs/hdfs-site.xml
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

# then run janus results
sed -i 's/<value>3</<value>2</g' ${HDFS_DIR}/scripts/configs/hdfs-site.xml
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