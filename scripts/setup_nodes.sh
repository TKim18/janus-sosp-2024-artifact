# This script is used to setup the Narwhal nodes for experiments.
# This includes setting up the high bandwidth network interface
# and formatting the raw block devices on each node.

# This script must likely be run by one of the submitters as it
# requires root access to execute some of the commands.

machines=(
"h0"
"h1" "h2" "h3" "h4" "h5" "h6" "h7" "h8" "h9" "h10" "h11" "h12" "h13"
"h14" "h15" "h16" "h17" "h18" "h19" "h20" "h21" "h22" "h23"
"h24" "h25" "h26" "h27")
nservers=28

i=0
while [ $i != $nservers ]
do
    ssh "${machines[i]}.disks.HeARTy" "sudo $(pwd)/node_actions/setup_node.sh" >> logs 2>> logs &
    i=$(($i+1))
done