#!/bin/bash

# define absolute path
workspace_dir="/proj/HeARTy/ceridwen-sosp-2024-artifact"

# move to hdfs scripts dir
cd "${workspace_dir}/hdfs/scripts"

# build hdfs from source
bash install-hdfs.sh

# copy configs to dist configs
cp configs/* "${workspace_dir}/hdfs/hadoop-dist/target/hadoop-3.3.1/etc/hadoop/"

# set env variables
export HADOOP_HOME="${workspace_dir}/hdfs/hadoop-dist/target/hadoop-3.3.1"
export PATH=$PATH:$HADOOP_HOME/bin
export PATH=$PATH:$HADOOP_HOME/sbin
export HADOOP_MAPRED_HOME=${HADOOP_HOME}
export HADOOP_COMMON_HOME=${HADOOP_HOME}
export HADOOP_YARN_HOME=${HADOOP_HOME}
export HADOOP_HDFS_HOME=${HADOOP_HOME}
export YARN_HOME=${HADOOP_HOME}
export PDSH_RCMD_TYPE=ssh
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"

# start cluster
"$HADOOP_HOME"/bin/hdfs namenode -format hdfs-cluster
"$HADOOP_HOME"/sbin/start-dfs.sh

# add policies
hdfs ec -addPolicies -policyFile "${workspace_dir}/hdfs/scripts/configs/user_ec_policies.xml"

# enable policies for Reed Solomon and Convertible Codes
hdfs ec -enablePolicy -policy RS-5-3-8192k
hdfs ec -enablePolicy -policy RS-6-3-8192k
hdfs ec -enablePolicy -policy RS-10-3-8192k
hdfs ec -enablePolicy -policy RS-12-3-8192k
hdfs ec -enablePolicy -policy RS-20-3-8192k

hdfs ec -enablePolicy -policy CC-5-3-8192k
hdfs ec -enablePolicy -policy CC-6-3-8192k
hdfs ec -enablePolicy -policy CC-10-3-8192k
hdfs ec -enablePolicy -policy CC-12-3-8192k
hdfs ec -enablePolicy -policy CC-20-3-8192k

# setup directories with EC policies
hdfs dfs -mkdir /ec53rs
hdfs ec -setPolicy -path /ec53rs -policy RS-5-3-8192k

hdfs dfs -mkdir /ec63rs
hdfs ec -setPolicy -path /ec63rs -policy RS-6-3-8192k

hdfs dfs -mkdir /ec103rs
hdfs ec -setPolicy -path /ec103rs -policy RS-10-3-8192k

hdfs dfs -mkdir /ec123rs
hdfs ec -setPolicy -path /ec123rs -policy RS-12-3-8192k

hdfs dfs -mkdir /ec203rs
hdfs ec -setPolicy -path /ec203rs -policy RS-20-3-8192k


hdfs dfs -mkdir /ec53cc
hdfs ec -setPolicy -path /ec53cc -policy RS-5-3-8192k

hdfs dfs -mkdir /ec63cc
hdfs ec -setPolicy -path /ec63cc -policy CC-6-3-8192k

hdfs dfs -mkdir /ec103cc
hdfs ec -setPolicy -path /ec103cc -policy RS-10-3-8192k

hdfs dfs -mkdir /ec123cc
hdfs ec -setPolicy -path /ec123cc -policy CC-12-3-8192k

hdfs dfs -mkdir /ec203cc
hdfs ec -setPolicy -path /ec203cc -policy RS-20-3-8192k