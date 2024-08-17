# This script can be run for both baseline and Ceridwen.
# Pass in either as a string to the first parameter.
# In this script since we're already on node h24, we have to
# 1. make sure the right properties are in and code set (dfs-perf can be in lifetime mode or vanilla mode)
# - for this, potentially just copy dfs-perf since it's two code-bases
# 2. make sure the config is set properly
# 3. make sure to build dfs perf from source
# 3. run write from node

./start_recording.sh
sleep 5

# for now let's assume it's just one code-base and it's the baseline workload

# build the project
cd ../dfs-perf
bash install-dfs-perf.sh

# execute the workload
./execute.sh
cd -

sleep 5
./stop_recording.sh