#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver
rootPath=/user/hadoop/user_category/lldaMahout
inputPath=$1
uidPath=$2
outputPath=$3
echo "hadoop jar $JAR $MAIN  --dict_root ${rootPath}/dictionary  --resource_root ${rootPath}/resources \
      --input $inputPath --output $outputPath --uid_path $uidPath "
hadoop jar $JAR $MAIN  --dict_root ${rootPath}/dictionary  --resource_root ${rootPath}/resources \
--input $inputPath --output $outputPath --uid_path $uidPath
