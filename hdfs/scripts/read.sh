echo "Start: $(date +"%T.%3N")"

time hdfs dfs -cat /dir0/random_16777216_0.txt > /dev/null

echo "End: $(date +"%T.%3N")"