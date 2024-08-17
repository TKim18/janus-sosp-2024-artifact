# This script is used to stop the telemetry processes and collect results.

machines=(
"h1" "h2" "h3" "h4" "h5" "h6" "h7" "h8" "h9" "h10" "h11" "h12" "h13"
"h14" "h15" "h16" "h17" "h18" "h19" "h20" "h21" "h22" "h23")
nservers=23

i=0
while [ $i != $nservers ]
do
    ssh "${machines[i]}.disks.HeARTy" "bash $(pwd)/resources/start_recording.sh $(pwd)/results" >> logs 2>> logs&
    i=$(($i+1))
done

# Results are all written out, use seekwatcher to record output
cd ../seekwatcher && python3 setup.py install && cd -
seekwatcher -t results/blktrace_raw/h