#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.mapreduce.LLDADriver
rootPath=/user/hadoop/user_category/lldaMahout
echo "hadoop jar $JAR $MAIN  --input ${rootPath}/docs/$1 -k 20 --output ${rootPath}/models --maxIter 4 -mipd 50 --dictionary ${rootPath}/dictionary/dict \
      -dt ${rootPath}/docTopics -mt ${rootPath}/tmpModels --num_reduce_tasks 1"
hadoop jar $JAR $MAIN  --input ${rootPath}/docs/$1 -k 21 --output ${rootPath}/models --maxIter 4 -mipd 50 --dictionary ${rootPath}/dictionary/dict \
-dt ${rootPath}/docTopics -mt ${rootPath}/tmpModels --num_reduce_tasks 1
