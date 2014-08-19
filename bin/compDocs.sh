#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.data.mergedocs.MergeLDocDriver
multiInput=$1
output=$2
uidFilePath=$3
logFile=/data0/log/user_category/processLog/llda/test/compDocs.log
echo `date` >> $logFile
echo "hadoop jar $JAR $MAIN --multi_input $multiInput --output $output --uid_path $uidFilePath --use_cookieId cookie >> $logFile 2>&1"
hadoop jar $JAR $MAIN --multi_input $multiInput --output $output --uid_path $uidFilePath --use_cookieId cookie >> $logFile 2>&1