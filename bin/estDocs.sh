#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.mapreduce.est.LLDADriver
rootPath=/user/hadoop/user_category/lldaMahout/test
inputDocs=$1
logFile=/data0/log/user_category/processLog/llda/test/est.log
now=`date`
echo $now >> $logFile
echo "hadoop jar $JAR $MAIN  --input $inputDocs -k 42 --output ${rootPath}/models --maxIter 40 -mipd 1 --dictionary ${rootPath}/dictionary/dict --resource_root ${rootPath}/resources \
      -dt ${rootPath}/docTopics/est -mt ${rootPath}/tmpModels --num_reduce_tasks 5 --num_train_threads 8 --num_update_threads 4 --test_set_fraction 0.1 --iteration_block_size 3 >> $logFile 2>&1"
hadoop jar $JAR $MAIN  --input $inputDocs -k 42 --output ${rootPath}/models --maxIter 40 -mipd 1 --dictionary ${rootPath}/dictionary/dict --resource_root ${rootPath}/resources \
-dt ${rootPath}/docTopics/est -mt ${rootPath}/tmpModels --num_reduce_tasks 5 --num_train_threads 8 --num_update_threads 4 --test_set_fraction 0.1 --iteration_block_size 3 >> $logFile 2>&1

