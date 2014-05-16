#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.data.complementdocs.ComplementLDocDriver
rootPath=/user/hadoop/user_category/lldaMahout
hadoop jar $JAR $MAIN --input $1 --leftInput $2 --docsRoot ${rootPath}/docs --dictionary ${rootPath}/dictionary