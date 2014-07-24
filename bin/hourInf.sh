#!/bin/bash
baseDir=`dirname $0`/..
startTime=`date +%Y%m%d%H%M%S -d "-2 hours"`
endTime=`date +%Y%m%d%H%M%S -d "-1 hours"`
preDay=`date +%Y%m%d -d "-1 days"`
day=`date +%Y%m%d `
logFile=/data/log/user_category/processLog/llda/hourInf.log
textInputRoot=url_count/all_projects
resultRoot=user_category/lldaMahout/docTopics
localResultRoot=/data/log/user_category_result/pr
now=`date`
echo $now >> $logFile
sh $baseDir/bin/accumulateUrlCount.sh $startTime $endTime >> $logFile
sh $baseDir/bin/crondInf.sh ${textInputRoot}/clean/${startTime}_${endTime} >> $logFile
sh $baseDir/bin/etl.sh ${resultRoot}/inf ${resultRoot}/inf_result ${localResultRoot} $startTime >> $logFile
