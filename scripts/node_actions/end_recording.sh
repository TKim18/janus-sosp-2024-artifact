#!/bin/bash

# stop blktrace process
kill "$(ps aux | grep 'blktrace' | awk '{print $2}')"

# stop space process
kill "$(ps fjx | grep 'sleep 10' | awk '{print $1}')"
proc=$(echo "$(ps fjx | grep 'sleep 10' | awk '{print $1}')" | sed -n '2 p')
echo $proc
kill -9 $proc