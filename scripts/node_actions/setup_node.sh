#!/bin/bash

#### add this to your startup script

# create evaluser
#  uid = 2001 (same as archived evaluser)
#  gid = 7058 (existing narwhal group for project sosp24eval)
#  home = /proj/sosp24eval/home/evaluser (already created, along with ssh keys)

sudo adduser --home /proj/sosp24eval/home/evaluser --shell /bin/bash -uid 2001 --gid 7058 --gecos "SOSP24 Eval User,,,,ttk2+sosp24emulab@andrew.cmu.edu" --disabled-password evaluser

# add sudoers.d file so evaluser can run anything under /proj/sosp24eval/janus-sosp-2024-artifact/scripts/
# contents of sosp24eval_sudoers ():
#	evaluser ALL=(ALL) NOPASSWD: /proj/sosp24eval/janus-sosp-2024-artifact/scripts/
#	evaluser ALL=(ALL) NOPASSWD: /proj/sosp24eval/janus-sosp-2024-artifact/scripts/node_actions/
#	evaluser ALL=(ALL) NOPASSWD: /proj/sosp24eval/janus-sosp-2024-artifact/scripts/baseline/
# TODO:  for safety, could check 0 retval of `visudo -c -f /proj/sosp24eval/sosp24eval_sudoers`)

sudo cp /proj/sosp24eval/sosp24eval_sudoers /etc/sudoers.d/sosp24eval_sudoers

# enable high-bandwidth NIC
sudo /share/testbed/bin/network --fge up

# format the block device
DISK=`lsblk | awk '!$1{print a}{a=$1}END{print a}'`
sudo mkfs.ext4 -E lazy_itable_init=0,lazy_journal_init=0 -C 1048576 -O bigalloc "/dev/${DISK}"
sudo mkdir /mnt/ext4
sudo mount "/dev/${DISK}" "/mnt/ext4"
sudo chown -R evaluser:root /mnt/ext4

echo "Done setting up nodes."