#!/bin/bash

current_dir=`pwd`
base_dir=$(cd `dirname $0`; pwd)

# JVM performance options
JVM_PERFORMANCE_OPTS="-server -Duser.timezone=GMT+08"

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
read -s -p "Enter your password:" firstPassword
read -s -p "Enter password again:" secondPassword

if [ "${firstPassword}x" == "${secondPassword}x" ]; then
    password="${firstPassword}"
    MAIN_CLASS="com.codejianhongxie.util.PasswordUtil"
    exec $JAVA $HEAP_OPTS $JVM_PERFORMANCE_OPTS $LOG4J_OPTS -cp $CLASSPATH $MAIN_CLASS "${password}"
else
    echo "password does not match"
fi



