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

# reminder/notes for creating tunnel on ops
#	ssh ttk2@ops
#	screen
#	ssh -R:2022:h24.evaldisks.sosp24eval:22 ops
# Ctrl+a,d   (to disconnect screen)
# ... a few moments later ...
#	ssh ttk2@ops
#	screen -r
# Ctrl+d to exit shell and kill ssh tunnel
# Ctrl+d again to exit shell and exit screen

# send sosp24eval.pem to reviewer(s)

# then SOSP reviewer(s) client ssh command:
#	ssh -p 2022 -l evaluser -i sosp24eval.pem ops.narwhal.pdl.cmu.edu

# enable high-bandwidth NIC
sudo /share/testbed/bin/network --fge up

cd /proj/sosp24eval/janus-sosp-2024-artifact/seekwatcher && sudo python3 setup.py install

echo "Done setting up nodes."