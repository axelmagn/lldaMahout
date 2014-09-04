#!/bin/bash
 baseDir=`dirname $0`/..
 JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
 sh $baseDir/bin/accumulateFuncs.sh
 funcName=$1
 shift
 $funcName $@