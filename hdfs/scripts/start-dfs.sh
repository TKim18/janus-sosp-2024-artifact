cp configs/* "${HDFS_DIR}/hadoop-dist/target/hadoop-3.3.1/etc/hadoop/"

# set env variables
source ~/.bashrc

# start cluster
yes Y | "$HADOOP_HOME"/bin/hdfs namenode -format hdfs-cluster
"$HADOOP_HOME"/sbin/start-dfs.sh
