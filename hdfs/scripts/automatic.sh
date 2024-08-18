./client_build.sh
cp big_configs/* ../hadoop-dist/target/hadoop-3.3.1/etc/hadoop/
./start.sh
source ~/.bashrc
./add_policies.sh
./smoke_test.sh
