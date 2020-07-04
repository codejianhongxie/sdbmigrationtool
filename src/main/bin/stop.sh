#!/bin/bash

PID=`ps -ef | grep "com.codejianhongxie.Engine" | grep -v grep | awk '{print $2}'`
if [ "${PID}x" == "x" ]; then
   echo "sdbmigrationtool is not running."
else
   echo "sdbmigrationtool is running, PID is ${PID}, ready to exit."
   kill -15 ${PID}
fi
