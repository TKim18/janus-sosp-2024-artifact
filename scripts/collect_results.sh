#!/bin/bash

# This script collects results. This must be run as sudo.

WORKLOAD=$1

# aggregate blktrace output
cd ${RESULTS_DIR}/"$WORKLOAD"/output
sudo /proj/sosp24eval/janus-sosp-2024-artifact/scripts/seekwatcher -t ${RESULTS_DIR}/"$WORKLOAD"/blktrace_raw/h

# aggregate space results
${SCRIPTS_DIR}/aggregate_space.py "$WORKLOAD"
