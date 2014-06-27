#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.crond.CrondInfDriver
rootPath=/user/hadoop/user_category/lldaMahout
inputPath=$1
echo "hadoop jar $JAR $MAIN --input $inputPath --doc_root ${rootPath}/docs --dict_root ${rootPath}/dictionary \
      --resource_root ${rootPath}/resources --num_topics 8 --model_input ${rootPath}/models --output ${rootPath}/docTopics/inf"
hadoop jar $JAR $MAIN --input $inputPath --doc_root ${rootPath}/docs --dict_root ${rootPath}/dictionary \
--resource_root ${rootPath}/resources --num_topics 8 --model_input ${rootPath}/models --output ${rootPath}/docTopics/inf