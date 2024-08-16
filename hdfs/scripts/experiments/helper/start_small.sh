# Startup server
source ~/.bashrc
$HADOOP_HOME/bin/hdfs namenode -format hdfs-cluster
$HADOOP_HOME/sbin/start-dfs.sh

hdfs ec -addPolicies -policyFile /proj/HeARTy/dare-hadoop/scripts/configs/user_ec_policies.xml
hdfs ec -enablePolicy -policy XOR-3-2-1024k
hdfs ec -enablePolicy -policy XOR-2-1-1024k
hdfs ec -enablePolicy -policy XOR-4-1-1024k
hdfs ec -enablePolicy -policy XOR-3-1-1024k

# Creates a bunch of erasure coded directories
hdfs dfs -mkdir /ec21
hdfs dfs -mkdir /ec31
hdfs dfs -mkdir /ec32
hdfs dfs -mkdir /ec41
hdfs ec -setPolicy -path /ec21 -policy XOR-2-1-1024k
hdfs ec -setPolicy -path /ec31 -policy XOR-3-1-1024k
hdfs ec -setPolicy -path /ec32 -policy XOR-3-2-1024k
hdfs ec -setPolicy -path /ec41 -policy XOR-4-1-1024k