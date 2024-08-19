#!/bin/bash
# Script input ($1) = output directory for raw traces on NFS mount

RESULTS_DIR="$1"
PARTITION=$(findmnt -n -o SOURCE --target /mnt/ext4)
HOSTNAME=$(hostname)
HOST=${HOSTNAME%.disks*}

# collect blktrace
TIME=3000 # setting a max in case stop recording isn't called
sudo blktrace -d "${PARTITION}" -D "${RESULTS_DIR}/blktrace_raw" -o "${HOST}-trace" -w "${TIME}" &

while true
do
	filesize="$(df -k /mnt/ext4 | awk 'NR > 1 {print $3}' | cut -d "%" -f 1)"
	echo "$filesize" >> "${RESULTS_DIR}/space/${HOSTNAME}"
	sleep 10
done