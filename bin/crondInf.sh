#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.mapreduce.crond.CrondInfDriver
rootPath=/user/hadoop/user_category/lldaMahout
textInputRoot=url_count/all_projects
inputPath=${textInputRoot}/$1
echo "hadoop jar $JAR $MAIN --input $inputPath --docsRoot ${rootPath}/docs --dict_root ${rootPath}/dictionary \
      --resourceDir ${rootPath}/resources --num_topics 8 --model_input ${rootPath}/models --output ${rootPath}/docTopics/inf"
hadoop jar $JAR $MAIN --input $inputPath --docsRoot ${rootPath}/docs --dict_root ${rootPath}/dictionary \
--resourceDir ${rootPath}/resources --num_topics 8 --model_input ${rootPath}/models --output ${rootPath}/docTopics/inf