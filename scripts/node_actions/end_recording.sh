#!/bin/bash

# stop blktrace process
ps aux | grep blktrace | grep -v "grep blktrace" | awk '{print $2}' | sudo xargs kill

# stop space process
proc=$(echo "$(ps fjx | grep 'sleep 10' | awk '{print $1}')" | sed -n '2 p')
echo $proc
sudo kill -9 $proc
