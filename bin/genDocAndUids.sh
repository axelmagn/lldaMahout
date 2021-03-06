#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver
rootPath=/user/hadoop/user_category/lldaMahout
inputPath=$1
uidPath=$3
outputPath=$2
logFile=/data0/log/user_category/processLog/llda/genDocsAndUids.log
echo "hadoop jar $JAR $MAIN  --dict_root ${rootPath}/dictionary  --resource_root ${rootPath}/resources \
      --input $inputPath --output $outputPath --uid_path $uidPath >> $logFile 2>&1"
hadoop jar $JAR $MAIN  --dict_root ${rootPath}/dictionary  --resource_root ${rootPath}/resources \
--input $inputPath --output $outputPath --uid_path $uidPath >> $logFile 2>&1
