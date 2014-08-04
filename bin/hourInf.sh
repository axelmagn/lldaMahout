#!/bin/bash
baseDir=`dirname $0`/..
startTime=`date +%Y%m%d%H%M%S -d "-1 hours -30 mins"`
endTime=`date +%Y%m%d%H%M%S -d "-30 mins"`
preDay=`date +%Y%m%d -d "-1 days"`
day=`date +%Y%m%d `
logFile=/data0/log/user_category/processLog/llda/hourInf.log
textInputRoot=url_count/all_projects
resultRoot=user_category/lldaMahout/docTopics
localResultRoot=/data0/log/user_category_result/pr
now=`date`
echo $now >> $logFile
sh $baseDir/bin/accumulateUrlCount.sh $startTime $endTime >> $logFile
sh $baseDir/bin/crondInf.sh ${textInputRoot}/clean/${startTime}_${endTime} >> $logFile
sh $baseDir/bin/etl.sh ${resultRoot}/inf ${resultRoot}/inf_result ${localResultRoot} ${startTime:0:10}0000 >> $logFile
