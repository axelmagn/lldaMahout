#!/bin/bash
baseDir=`dirname $0`/..
day=`date +%Y%m%d `
logFile=/data/log/user_category/processLog/llda/fillUpInf.log
textInputRoot=url_count/all_projects
resultRoot=user_category/lldaMahout/docTopics
localResultRoot=/data/log/user_category_result/pr
startTime=$1
endTime=$2
now=`date`
echo $now >> $logFile
sh $baseDir/bin/accumulateUrlCount.sh $startTime $endTime >> $logFile
sh $baseDir/bin/crondInf.sh ${textInputRoot}/clean/${startTime}_${endTime} >> $logFile
sh $baseDir/bin/etl.sh ${resultRoot}/inf ${resultRoot}/inf_result ${localResultRoot} $startTime >> $logFile