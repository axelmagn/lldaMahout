#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.mapreduce.inf.LLDAInfDriver
rootPath=/user/hadoop/user_category/lldaMahout
echo "hadoop jar $JAR $MAIN  --input ${rootPath}/docs/$1 -k 42 --output ${rootPath}/models --maxIter 40 -mipd 1 --dictionary ${rootPath}/dictionary/dict  --resource_root ${rootPath}/resources \
      -dt ${rootPath}/docTopics/inf -mt ${rootPath}/tmpModels --num_reduce_tasks 1 --num_train_threads 8 --num_update_threads 4 --test_set_fraction 0.1 --iteration_block_size 4"
hadoop jar $JAR $MAIN  --input ${rootPath}/docs/$1 -k 42 --output ${rootPath}/models --maxIter 40 -mipd 1 --dictionary ${rootPath}/dictionary/dict --resource_root ${rootPath}/resources  \
-dt ${rootPath}/docTopics/inf -mt ${rootPath}/tmpModels --num_train_threads 8 --num_update_threads 4 --test_set_fraction 0.1 --iteration_block_size 4
