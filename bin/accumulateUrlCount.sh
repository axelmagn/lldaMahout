#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.data.accumulateurlcount.Accumulate
outputBase=url_count/all_projects
echo "hadoop jar $JAR -Xss256k $MAIN --outputBase $outputBase --startTime $1 --endTime $2"
hadoop jar $JAR -Xss256k $MAIN --outputBase $outputBase --startTime $1 --endTime $2