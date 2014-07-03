#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.mapreduce.etl.ResultEtlDriver
rootPath=/user/hadoop/user_category/lldaMahout
logFile=/data/log/user_category/processLog/llda/etl.log
if [ $# -lt 3 ];then
  echo "hadoop jar $JAR $MAIN --input ${rootPath}/docTopics/$1 --output ${rootPath}/docTopics/$2 \
  --local_result_root /data/log/user_category_result/pr >> $logFile 2>&1"
  hadoop jar $JAR $MAIN --input ${rootPath}/docTopics/$1 --output ${rootPath}/docTopics/$2 \
  --local_result_root /data/log/user_category_result/pr  >> $logFile 2>&1
else
  echo "hadoop jar $JAR $MAIN --input ${rootPath}/docTopics/$1 --output ${rootPath}/docTopics/$2 \
  --local_result_root /data/log/user_category_result/pr --result_time $3 >> $logFile 2>&1"
  hadoop jar $JAR $MAIN --input ${rootPath}/docTopics/$1 --output ${rootPath}/docTopics/$2 \
  --local_result_root /data/log/user_category_result/pr --result_time $3 >> $logFile 2>&1
fi
