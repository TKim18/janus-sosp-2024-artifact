#!/bin/bash

# re-initialize results directory
sudo mkdir -p ${RESULTS_DIR}

cd ${RESULTS_DIR}

WORKLOAD=baseline
sudo rm -rf "$WORKLOAD"
sudo mkdir -p "$WORKLOAD"/blktrace_raw
sudo mkdir -p "$WORKLOAD"/output
sudo mkdir -p "$WORKLOAD"/space

WORKLOAD=janus
sudo rm -rf "$WORKLOAD"
sudo mkdir -p "$WORKLOAD"/blktrace_raw
sudo mkdir -p "$WORKLOAD"/output
sudo mkdir -p "$WORKLOAD"/space

cd -