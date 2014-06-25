#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.data.complementdocs.MergeLDocDriver
multiInput=$1
output=$2
uidFilePath=$3
hadoop jar $JAR $MAIN --multi_input $multiInput --output $output --uid_path $uidFilePath