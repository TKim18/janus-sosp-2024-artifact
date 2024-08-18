hdfs ec -addPolicies -policyFile "/proj/HeARTy/ceridwen-sosp-2024-artifact/hdfs/scripts/configs/user_ec_policies.xml"

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
hdfs ec -setPolicy -path /ec103cc -policy CC-10-3-8192k

hdfs dfs -mkdir /ec123cc
hdfs ec -setPolicy -path /ec123cc -policy CC-12-3-8192k

hdfs dfs -mkdir /ec203cc
hdfs ec -setPolicy -path /ec203cc -policy CC-20-3-8192k

