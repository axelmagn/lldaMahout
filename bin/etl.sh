#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.mapreduce.ResultEtlDriver
rootPath=/user/hadoop/user_category/lldaMahout
if [ $# -lt 3 ];then
  echo "hadoop jar $JAR $MAIN --input ${rootPath}/docTopics/$1 --output ${rootPath}/docTopics/$2 \
  --localResultPath /data/log/user_category_result/pr "
  hadoop jar $JAR $MAIN --input ${rootPath}/docTopics/$1 --output ${rootPath}/docTopics/$2 \
  --localResultPath /data/log/user_category_result/pr
else
  echo "hadoop jar $JAR $MAIN --input ${rootPath}/docTopics/$1 --output ${rootPath}/docTopics/$2 \
  --localResultPath /data/log/user_category_result/pr --result_time $3 "
  hadoop jar $JAR $MAIN --input ${rootPath}/docTopics/$1 --output ${rootPath}/docTopics/$2 \
  --localResultPath /data/log/user_category_result/pr --result_time $3
fi
