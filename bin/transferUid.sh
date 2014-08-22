#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.data.transferdocs.TransferUidDriver
logFile=/data0/log/user_category/processLog/llda/transferUid.log
inputPath=$1
outputPath=$2
echo "hadoop jar $JAR $MAIN --input $1 --output $2  >> $logFile 2>&1 "
hadoop jar $JAR $MAIN --input $1 --output $2  >> $logFile 2>&1
