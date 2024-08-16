#hdfs dfs -mkdir /ec42cc
#hdfs ec -setPolicy -path /ec42cc -policy CC-4-2-1024k
#
#hdfs dfs -mkdir /ec42rs
#hdfs ec -setPolicy -path /ec42rs -policy RS-4-2-1024k
#
#hdfs dfs -mkdir /ec41rs
#hdfs ec -setPolicy -path /ec41rs -policy RS-4-1-1024k
#
#hdfs dfs -mkdir /ec63cc
#hdfs ec -setPolicy -path /ec63cc -policy CC-6-3-1024k
#

# these below are good for cellsize=1MB
#hdfs dfs -mkdir /ec21rs
#hdfs ec -setPolicy -path /ec21rs -policy RS-2-1-1024k
#
#hdfs dfs -mkdir /ec63rs
#hdfs ec -setPolicy -path /ec63rs -policy RS-6-3-1024k
#
#hdfs dfs -mkdir /ec123rs
#hdfs ec -setPolicy -path /ec123rs -policy RS-12-3-1024k
#
#hdfs dfs -mkdir /ec63cc
#hdfs ec -setPolicy -path /ec63cc -policy CC-6-3-1024k
#
#hdfs dfs -mkdir /ec123cc
#hdfs ec -setPolicy -path /ec123cc -policy CC-12-3-1024k

# these below are good for cellsize=blocksize
#hdfs dfs -mkdir /ec21rs
#hdfs ec -setPolicy -path /ec21rs -policy RS-2-1-8192k
#
#hdfs dfs -mkdir /ec63rs
#hdfs ec -setPolicy -path /ec63rs -policy RS-6-3-8192k
#
#hdfs dfs -mkdir /ec123rs
#hdfs ec -setPolicy -path /ec123rs -policy RS-12-3-8192k
#
#hdfs dfs -mkdir /ec63cc
#hdfs ec -setPolicy -path /ec63cc -policy CC-6-3-8192k
#
#hdfs dfs -mkdir /ec123cc
#hdfs ec -setPolicy -path /ec123cc -policy CC-12-3-8192k

# these below are good for cellsize=1MB but with varying stripe widths
hdfs dfs -mkdir /ec63rs_3
hdfs ec -setPolicy -path /ec63rs_3 -policy RS-6-3-1024k

hdfs dfs -mkdir /ec63rs_6
hdfs ec -setPolicy -path /ec63rs_6 -policy RS-6-3-1024k

hdfs dfs -mkdir /ec63rs_12
hdfs ec -setPolicy -path /ec63rs_12 -policy RS-6-3-1024k

# these below are good for lifetime experiments
#hdfs dfs -mkdir /ec53rs
#hdfs ec -setPolicy -path /ec53rs -policy RS-5-3-8192k
#
#hdfs dfs -mkdir /ec103rs
#hdfs ec -setPolicy -path /ec103rs -policy RS-10-3-8192k
#
#hdfs dfs -mkdir /ec203rs
#hdfs ec -setPolicy -path /ec203rs -policy RS-20-3-8192k