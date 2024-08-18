# This script can be run for both baseline and Ceridwen.
# Pass in either as a string to the first parameter.
# In this script since we're already on node h24, we have to
# 1. make sure the right properties are in and code set (dfs-perf can be in lifetime mode or vanilla mode)
# - for this, potentially just copy dfs-perf since it's two code-bases
# 2. make sure the config is set properly
# 3. make sure to build dfs perf from source
# 3. run write from node

# first run baseline results
./begin_recording.sh baseline
sleep 5

cd ../dfs-perf

# set config
sed 's/ec53cc/rr3/g' conf/dfs-perf-env.sh

# build the project
bash install-dfs-perf.sh

# execute the workload
./execute.sh
cd -

sleep 5
./stop_recording.sh baseline

# then run ceridwen results
