#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
oneDayAgo=`date +%Y%m%d -d "-1 days"`
twoDayAgo=`date +%Y%m%d -d "-2 days"`
source $baseDir/bin/infEstFuncs.sh
updateEstByDay $oneDayAgo
source $baseDir/bin/categoryAnalysisFuncs.sh
anaInfResult $oneDayAgo
updateAnaCategoryDist $oneDayAgo


