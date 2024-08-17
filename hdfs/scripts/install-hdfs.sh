export MAVEN_OPTS="-Xms256m -Xmx1536m"
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
cd ..
mvn clean install -e -Pdist,native -DskipTests -Dmaven.javadoc.skip=true
cd -