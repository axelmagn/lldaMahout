#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.data.accumulateurlcount.Accumulate
outputBase=url_count/all_projects
logFile=/data/log/user_category/processLog/llda/accumulate.log
echo "hadoop jar $JAR  $MAIN --outputBase $outputBase --startTime $1 --endTime $2 >> $logFile 2>&1"
hadoop jar $JAR  $MAIN --outputBase $outputBase --startTime $1 --endTime $2  >> $logFile 2>&1