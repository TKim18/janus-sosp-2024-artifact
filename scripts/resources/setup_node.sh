#!/bin/bash

# TODO: remove (use this to confirm the script is working)
sudo mkdir /mnt/test
exit 0

# enable high-bandwidth NIC
/share/testbed/bin/network --fge up

# format the block device
DISK=`lsblk | awk '!$1{print a}{a=$1}END{print a}'`
sudo mkfs.ext4 -E lazy_itable_init=0,lazy_journal_init=0 -C 1048576 -O bigalloc "/dev/${DISK}"
sudo mkdir /mnt/ext4
sudo mount "/dev/${DISK}" "/mnt/ext4"
sudo chown -R ttk2:root /mnt/ext4
