# ceridwen-sosp-2024-artifact

For our experiments, h0 is the Namenode, h1-h23 are the 23 DataNodes, and the rest are client nodes.
All scripts will be originally executed from h24 (the first Client node).

## Set up evaluation cluster
Run `sudo ./setup_nodes.sh`.

## Set up HDFS cluster
Run `sudo ./start_hdfs_cluster.sh`.

## Run experiments
Run `sudo ./run_experiment_fig10b.sh`.

## View experiment results
The aggregated results of your experiment should be under `results/<experiment_name>/output`.

