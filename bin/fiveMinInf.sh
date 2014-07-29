#!/bin/bash
baseDir=`dirname $0`/..
startTime=`date +%Y%m%d%H%M%S -d "-10 mins"`
endTime=`date +%Y%m%d%H%M%S -d "-5 mins"`
preDay=`date +%Y%m%d -d "-1 days"`
day=`date +%Y%m%d `
logFile=/data0/log/user_category/processLog/llda/hadoopLLDA.log
echo "fiveMinInf $startTime" >> $logFile
textInputRoot=url_count/all_projects
resultRoot=user_category/lldaMahout/docTopics
localResultRoot=/data0/log/user_category_result/pr
#sh $baseDir/../Url_Count/bin/AccumulateUrlCount.sh $startTime $endTime >> $logFile
sh $baseDir/bin/accumulateUrlCount.sh $startTime $endTime >> $logFile
sh $baseDir/bin/crondInf.sh ${textInputRoot}/clean/${startTime}_${endTime}
sh $baseDir/bin/etl.sh ${resultRoot}/inf ${resultRoot}/inf_result ${localResultRoot} $startTime >> $logFile
