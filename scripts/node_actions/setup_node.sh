#!/bin/bash

# enable high-bandwidth NIC
sudo /share/testbed/bin/network --fge up

# format the block device
DISK=`lsblk | awk '!$1{print a}{a=$1}END{print a}'`
sudo mkfs.ext4 -E lazy_itable_init=0,lazy_journal_init=0 -C 1048576 -O bigalloc "/dev/${DISK}"
sudo mkdir /mnt/ext4
sudo mount "/dev/${DISK}" "/mnt/ext4"
sudo chown -R $USER:root /mnt/ext4
