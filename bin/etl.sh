#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.mapreduce.ResultEtlDriver
rootPath=/user/hadoop/user_category/lldaMahout
echo "hadoop jar $JAR $MAIN --input ${rootPath}/docTopics/$1 --output ${rootPath}/docTopics/$2 --localResultPath /data/log/user_category_result/pr
"
hadoop jar $JAR $MAIN --input ${rootPath}/docTopics/$1 --output ${rootPath}/docTopics/$2 --localResultPath /data/log/user_category_result/pr
