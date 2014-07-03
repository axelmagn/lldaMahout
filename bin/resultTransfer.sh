#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.mapreduce.etl.ResultTransfer
resultRoot=/data/log/user_category_result/pr
resultFile=$1
hadoop jar $JAR $MAIN ${resultRoot}/${resultFile}