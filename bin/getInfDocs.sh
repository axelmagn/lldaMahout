#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.data.preparedocs.PrepareInfDocsDriver
rootPath=/user/hadoop/user_category/lldaMahout
inputPath=$1
preLDocsPath=$2
lDocsOutPath=$3
echo "hadoop jar $JAR $MAIN --input $inputPath --dictionary ${rootPath}/dictionary --docsRoot ${rootPath}/docs --leftInput ${preLDocsPath} --docsDir ${lDocsOutPath} --resourceDir ${rootPath}/resources  "
hadoop jar $JAR $MAIN --input $inputPath --dictionary ${rootPath}/dictionary --docsRoot ${rootPath}/docs --leftInput ${preLDocsPath} --docsDir ${lDocsOutPath} --resourceDir ${rootPath}/resources
