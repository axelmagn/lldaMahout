#!/bin/bash
 baseDir=`dirname $0`/..
 JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
 source $baseDir/bin/categoryAnalysisFuncs.sh
 source $baseDir/bin/commonAnalysisFuncs.sh
 funcName=$1
 shift
 $funcName $@