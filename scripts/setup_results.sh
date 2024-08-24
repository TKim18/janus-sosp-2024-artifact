#!/bin/bash

# re-initialize results directory
sudo mkdir -p ${RESULTS_DIR}

cd ${RESULTS_DIR}

sudo rm -rf "$WORKLOAD"/blktrace_raw
sudo rm -rf "$WORKLOAD"/output
sudo rm -rf "$WORKLOAD"/space
sudo mkdir -p "$WORKLOAD"/blktrace_raw
sudo mkdir -p "$WORKLOAD"/output
sudo mkdir -p "$WORKLOAD"/space

cd -