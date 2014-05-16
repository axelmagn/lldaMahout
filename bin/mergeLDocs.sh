#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.data.mergedocs.MergeLDocDriver
rootPath=/user/hadoop/user_category/lldaMahout
hadoop jar $JAR $MAIN --input ${rootPath}/docs/$1 --leftInput $2 --docsRoot ${rootPath}/docs --dictionary ${rootPath}/dictionary