cp configs/* "/proj/sosp24eval/janus-sosp-2024-artifact/hdfs/hadoop-dist/target/hadoop-3.3.1/etc/hadoop/"

# set env variables
export HADOOP_HOME="/proj/sosp24eval/janus-sosp-2024-artifact/hdfs/hadoop-dist/target/hadoop-3.3.1"
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
export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64"
export PDSH_RCMD_TYPE="ssh"

# start cluster
yes Y | "$HADOOP_HOME"/bin/hdfs namenode -format hdfs-cluster
"$HADOOP_HOME"/sbin/start-dfs.sh
