# ceridwen-sosp-2024-artifact

For our experiments, h0 is the Namenode, h1-h23 are the 23 DataNodes, and the rest are client nodes.

All scripts will be originally executed from h24 (the first Client node).

Have an output directory ready to collect all data. Put it somewhere on the NFS mount to make it easier to 
collect all the data into a single location. We recommend making a subdirectory in this artifact directory
and pointing the recording to the subdirectory.