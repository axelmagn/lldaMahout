#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.dictionary.UpdateRegularDictDriver
rootPath=/user/hadoop/user_category/lldaMahout
echo "hadoop jar $JAR $MAIN --input $1 --dictionary ${rootPath}/regularDict"
hadoop jar $JAR $MAIN --input $1 --dictionary ${rootPath}/regularDict