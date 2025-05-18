#!/bin/bash
before=`/usr/bin/docker exec spark-master /bin/bash -c 'stat -c %s /opt/bitnami/spark/examples/jars/sparking-1.0-SNAPSHOT.jar'`
echo "Before: $before"
docker cp build/libs/sparking-1.0-SNAPSHOT.jar spark-master:/opt/bitnami/spark/examples/jars/.
cd src/main/java
docker cp br spark-master:/opt/bitnami/spark/examples/src/main/java
cd -
after=`/usr/bin/docker exec spark-master /bin/bash -c 'stat -c %s /opt/bitnami/spark/examples/jars/sparking-1.0-SNAPSHOT.jar'`
echo "After: $after"
