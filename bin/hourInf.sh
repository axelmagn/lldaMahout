#!/bin/bash
baseDir=`dirname $0`/..
startTime=`date +%Y%m%d%H%M%S -d "-1 hours -20 mins"`
endTime=`date +%Y%m%d%H%M%S -d "-20 mins"`
preDay=`date +%Y%m%d -d "-1 days"`
day=`date +%Y%m%d `
logFile=/data0/log/user_category/processLog/llda/test/hourInf.log
textInputRoot=url_count/all_projects
resultRoot=user_category/lldaMahout/test/docTopics
localResultRoot=/data0/log/user_category_result/pr/test
now=`date`
echo $now >> $logFile
sh $baseDir/bin/accumulateUrlCount.sh $startTime $endTime >> $logFile
sh $baseDir/bin/crondInf.sh ${textInputRoot}/clean/${startTime}_${endTime} >> $logFile
sh $baseDir/bin/etl.sh ${resultRoot}/inf ${resultRoot}/inf_result ${localResultRoot} ${startTime:0:10}0000 >> $logFile
