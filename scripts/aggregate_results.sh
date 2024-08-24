#!/bin/bash

# This script collects results. This must be run as root.

# aggregate blktrace output
pip3 install matplotlib
pip3 install numpy

# aggregate results for baseline
WORKLOAD=baseline
cd ${RESULTS_DIR}/"$WORKLOAD"/output
sudo /proj/sosp24eval/janus-sosp-2024-artifact/scripts/seekwatcher -t ${RESULTS_DIR}/"$WORKLOAD"/blktrace_raw/h
${SCRIPTS_DIR}/aggregate_space.py "$WORKLOAD"

# aggregate results for janus
WORKLOAD=janus
cd ${RESULTS_DIR}/"$WORKLOAD"/output
sudo /proj/sosp24eval/janus-sosp-2024-artifact/scripts/seekwatcher -t ${RESULTS_DIR}/"$WORKLOAD"/blktrace_raw/h
${SCRIPTS_DIR}/aggregate_space.py "$WORKLOAD"