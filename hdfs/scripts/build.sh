export MAVEN_OPTS="-Xms256m -Xmx1536m"
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
cd ..
mvn clean package -e -Pdist -DskipTests -Dtar -Dmaven.javadoc.skip=true \
    -Drequire.isal -Disal.lib=/usr/lib/ -Dbundle.isal \
    -Drequire.snappy -Dsnappy.lib=/usr/lib/x86_64-linux-gnu/ -Dbundle.snappy \
    -Drequire.openssl -Dopenssl.lib=/usr/lib/x86_64-linux-gnu/ -Dbundle.openssl

cd scripts
# copy config files into binary
cp local_configs/* ../hadoop-dist/target/hadoop-3.3.1/etc/hadoop/
