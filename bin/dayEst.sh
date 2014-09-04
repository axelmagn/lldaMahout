#!/bin/bash
baseDir=`dirname $0`/..
oneDayAgo=`date +%Y%m%d -d "-1 days"`
twoDayAgo=`date +%Y%m%d -d "-2 days"`
sh $baseDir/bin/infEstFuncs.sh
updateEstByDay $oneDayAgo
sh $baseDir/bin/categoryAnalysisFuncs.sh
anaInfResult $oneDayAgo
updateAnaCategoryDist $oneDayAgo


