# Startup server
source ~/.bashrc
$HADOOP_HOME/bin/hdfs namenode -format hdfs-cluster
$HADOOP_HOME/sbin/start-dfs.sh

hdfs ec -addPolicies -policyFile /proj/HeARTy/dare-hadoop/scripts/big_configs/user_ec_policies.xml

hdfs ec -enablePolicy -policy XOR-3-1-1024k
hdfs ec -enablePolicy -policy XOR-4-1-1024k
hdfs ec -enablePolicy -policy XOR-5-1-1024k
hdfs ec -enablePolicy -policy XOR-6-1-1024k
hdfs ec -enablePolicy -policy XOR-7-1-1024k
hdfs ec -enablePolicy -policy XOR-8-1-1024k
hdfs ec -enablePolicy -policy XOR-9-1-1024k
hdfs ec -enablePolicy -policy XOR-10-1-1024k
hdfs ec -enablePolicy -policy XOR-11-1-1024k
hdfs ec -enablePolicy -policy XOR-12-1-1024k
hdfs ec -enablePolicy -policy XOR-13-1-1024k
hdfs ec -enablePolicy -policy XOR-14-1-1024k
hdfs ec -enablePolicy -policy XOR-15-1-1024k

#hdfs ec -enablePolicy -policy RS-6-3-1024k
#hdfs ec -enablePolicy -policy RS-7-3-1024k
#hdfs ec -enablePolicy -policy RS-8-3-1024k
#hdfs ec -enablePolicy -policy RS-9-3-1024k
#hdfs ec -enablePolicy -policy RS-10-3-1024k
#hdfs ec -enablePolicy -policy RS-11-3-1024k
#hdfs ec -enablePolicy -policy RS-12-3-1024k
#hdfs ec -enablePolicy -policy RS-13-3-1024k
#hdfs ec -enablePolicy -policy RS-14-3-1024k

# Creates a bunch of erasure coded directories
hdfs dfs -mkdir /ec31
hdfs dfs -mkdir /ec41
hdfs dfs -mkdir /ec51
hdfs dfs -mkdir /ec61
hdfs dfs -mkdir /ec71
hdfs dfs -mkdir /ec81
hdfs dfs -mkdir /ec91
hdfs dfs -mkdir /ec101
hdfs dfs -mkdir /ec111
hdfs dfs -mkdir /ec121
hdfs dfs -mkdir /ec131
hdfs dfs -mkdir /ec141
hdfs dfs -mkdir /ec151

# Creates a bunch of erasure coded directories
#hdfs dfs -mkdir /ec63
#hdfs dfs -mkdir /ec73
#hdfs dfs -mkdir /ec83
#hdfs dfs -mkdir /ec93
#hdfs dfs -mkdir /ec103
#hdfs dfs -mkdir /ec113
#hdfs dfs -mkdir /ec123
#hdfs dfs -mkdir /ec133
#hdfs dfs -mkdir /ec143

hdfs ec -setPolicy -path /ec31 -policy XOR-3-1-1024k
hdfs ec -setPolicy -path /ec41 -policy XOR-4-1-1024k
hdfs ec -setPolicy -path /ec51 -policy XOR-5-1-1024k
hdfs ec -setPolicy -path /ec61 -policy XOR-6-1-1024k
hdfs ec -setPolicy -path /ec71 -policy XOR-7-1-1024k
hdfs ec -setPolicy -path /ec81 -policy XOR-8-1-1024k
hdfs ec -setPolicy -path /ec91 -policy XOR-9-1-1024k
hdfs ec -setPolicy -path /ec101 -policy XOR-10-1-1024k
hdfs ec -setPolicy -path /ec111 -policy XOR-11-1-1024k
hdfs ec -setPolicy -path /ec121 -policy XOR-12-1-1024k
hdfs ec -setPolicy -path /ec131 -policy XOR-13-1-1024k
hdfs ec -setPolicy -path /ec141 -policy XOR-14-1-1024k
hdfs ec -setPolicy -path /ec151 -policy XOR-15-1-1024k

#hdfs ec -setPolicy -path /ec63 -policy RS-6-3-1024k
#hdfs ec -setPolicy -path /ec73 -policy RS-7-3-1024k
#hdfs ec -setPolicy -path /ec83 -policy RS-8-3-1024k
#hdfs ec -setPolicy -path /ec93 -policy RS-9-3-1024k
#hdfs ec -setPolicy -path /ec103 -policy RS-10-3-1024k
#hdfs ec -setPolicy -path /ec113 -policy RS-11-3-1024k
#hdfs ec -setPolicy -path /ec123 -policy RS-12-3-1024k
#hdfs ec -setPolicy -path /ec133 -policy RS-13-3-1024k
#hdfs ec -setPolicy -path /ec143 -policy RS-14-3-1024k

## Startup server
#source ~/.bashrc
#$HADOOP_HOME/bin/hdfs namenode -format hdfs-cluster
#$HADOOP_HOME/sbin/start-dfs.sh
#
#hdfs ec -addPolicies -policyFile /proj/HeARTy/dare-hadoop/scripts/big_configs/user_ec_policies_large.xml
#