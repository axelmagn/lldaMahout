#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver
rootPath=/user/hadoop/user_category/lldaMahout
echo "hadoop jar $JAR $MAIN --input $1 --docsRoot ${rootPath}/docs --docsDir $2 --dictionary ${rootPath}/dictionary  --resourceDir ${rootPath}/resources "
hadoop jar $JAR $MAIN --input $1 --docsRoot ${rootPath}/docs --docsDir $2 --dictionary ${rootPath}/dictionary  --resourceDir ${rootPath}/resources
