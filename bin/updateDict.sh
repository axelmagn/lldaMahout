#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.data.dictionary.UpdateDictDriver
rootPath=/user/hadoop/user_category/lldaMahout
echo "hadoop $JAR $MAIN --input $1 --dictionary ${rootPath}/dictionary"
hadoop $JAR $MAIN --input $1 --dictionary ${rootPath}/dictionary