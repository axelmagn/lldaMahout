#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.data.accumulateurlcount.Accumulate
outputBase=url_count/all_projects
logFile=/data/log/user_category/processLog/llda/accumulate.log
echo `date` >> $logFile
echo "hadoop jar $JAR  $MAIN --outputBase $outputBase --startTime $1 --endTime $2 >> $logFile 2>&1"
hadoop jar $JAR  $MAIN --outputBase $outputBase --startTime $1 --endTime $2  >> $logFile 2>&1
echo "sh ${baseDir}/bin/analysis wordClean ${outputBase}/$1_$2 ${outputBase}/clean/$1_$2"
sh ${baseDir}/bin/analysis.sh wordClean ${outputBase}/$1_$2 ${outputBase}/clean/$1_$2