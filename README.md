# Janus
Janus is implemented as an augmented version of HDFS with capabilities for hybrid redundancy, natively 
supported transcoding operations, and a Convertible Codes implementation. All of these features and their
implementations are visible in the hdfs subdirectory, which builds from source.

We use dfs-perf as a tool to generate load and trigger transcode operations on the DFS systematically.
We use seekwatcher as a tool to analyze disk bandwidth usage.

## HDFS Cluster Requirements
For the purposes of this evaluation, we use a private academic cluster with 29 nodes, named h0-28.
For our experiments, h0 is the Namenode, h1-h23 are the 23 DataNodes, and the rest are client nodes.
All scripts will be executed from h24 (the first Client node).

We will provide access credentials and instructions to the cluster to the evaluator privately via HotCRP.
However, please notify us through HotCRP before running experiments such that we can allocate the necessary resources.

## Evaluating Janus
These are the steps after the cluster is spun up to run and get results for Figure 10b in the attached paper.
This figure is a macrobenchmark that evaluates all relevant aspects of Janus (transcoding latency, bandwidth, capacity
savings, end-to-end latency savings). Once ssh'ed into the academic cluster node, please follow the steps below to
execute and collect results. Note that the repository is already cloned at a globally available directory.

### Steps
1. Run `cd /proj/sosp24eval/janus-sosp-2024-artifact/scripts`. All of these scripts are available for you to run.
2. Run `cat env > ~/.bashrc`.
3. Run `source ~/.bashrc`.
4. Run `./start_hdfs_cluster.sh`. This will build Janus from source and spin up an HDFS cluster using the Janus implementation.
5. Run `mkdir ../results`.
6. Run `./run_experiment_fig10b.sh`. This will execute the experiment for Figure 10b.

### Accessing and interpreting results
The aggregated results of the experiment can be found at `../results/baseline/output` and `../results/janus/output`.
There will be 3 files in each directory:
1. total_space: the total aggregate space consumed by the files.
2. trace.png: graphs of the disk I/O patterns.
3. tput.csv: the data of the throughput graph in trace.png in csv form.

It is likely (or probable) that the disk patterns do not look identical. That is simply due to the nondeterministic nature of
our experiment and disk behavior.

# Contact
Please contact ttk2@cs.cmu.edu for any issues/concerns/questions regarding the code or setup.


