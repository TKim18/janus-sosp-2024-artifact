export MAVEN_OPTS="-Xms256m -Xmx1536m"
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
export http_proxy=http://proxy.pdl.cmu.edu:3128/
export https_proxy=http://proxy.pdl.cmu.edu:3128/

cd ..
mvn clean install -Pdist,native -DskipTests -Dmaven.javadoc.skip=true -Drequire.isal -Disal.lib=/usr/lib/ -Dbundle.isal
cd ../scripts
