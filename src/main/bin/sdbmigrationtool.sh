#!/bin/bash

current_dir=`pwd`
base_dir=$(cd `dirname $0`; pwd)

# Memory options
HEAP_OPTS="-Xmx1024M -Xms1024M"
# JVM performance options
JVM_PERFORMANCE_OPTS="-server -Duser.timezone=GMT+08"
# LOG4j options
LOG4J_OPTS="-Dlog4j.configurationFile=$base_dir/../conf/log4j.properties"

for file in $base_dir/../lib/*
do
    CLASSPATH=$CLASSPATH:$file
done

if [ -z "$JAVA_HOME" ]; then
    JAVA="java"
else
    JAVA="$JAVA_HOME/bin/java"
fi
cd ${base_dir}
MAIN_CLASS="com.codejianhongxie.Engine"
exec $JAVA $HEAP_OPTS $JVM_PERFORMANCE_OPTS $LOG4J_OPTS -cp $CLASSPATH $MAIN_CLASS "$@"
