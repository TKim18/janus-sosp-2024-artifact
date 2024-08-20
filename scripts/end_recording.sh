# This script is used to stop the telemetry processes and collect results.

WORKLOAD=$1

machines=(
"h1" "h2" "h3" "h4" "h5" "h6" "h7" "h8" "h9" "h10" "h11" "h12" "h13"
"h14" "h15" "h16" "h17" "h18" "h19" "h20" "h21" "h22" "h23")
nservers=23

i=0
while [ $i != $nservers ]
do
    ssh "${machines[i]}.disks.HeARTy" "bash $(pwd)/node_actions/end_recording.sh $(pwd)/results" &
    i=$(($i+1))
done

# Results are all written out, aggregate all data
cd ../seekwatcher && sudo python3 setup.py install && cd -
cd results/"$WORKLOAD"/output

# aggregate blktrace output
seekwatcher -t /proj/HeARTy/ceridwen-sosp-2024-artifact/scripts/results/"$WORKLOAD"/blktrace_raw/h

cd -
# aggregate space results
python3 aggregate_space.py "$WORKLOAD"

cd -
