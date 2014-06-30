#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.data.mergedocs.MergeLDocDriver
multiInput=$1
output=$2
logFile=logFile=/data/log/user_category/processLog/llda/mergeDocs.log
echo `date` >> $logFile
echo "hadoop jar $JAR $MAIN --multi_input $multiInput --output $output >> $logFile"
hadoop jar $JAR $MAIN --multi_input $multiInput --output $output >> $logFile