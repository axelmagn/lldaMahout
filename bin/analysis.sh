#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.AnalysisDriver
rootPath=/user/hadoop/user_category/lldaMahout
logFile=/data/log/user_category/processLog/llda/analysis.log
echo "hadoop jar $JAR $MAIN --input ${rootPath}/$1 --output ${rootPath}/$2 >> $logFile 2>&1"
hadoop jar $JAR $MAIN --input ${rootPath}/$1 --output ${rootPath}/$2 >> $logFile 2>&1
