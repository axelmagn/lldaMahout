#!/bin/bash
baseDir=`dirname $0`/..
startTime=`date +%Y%m%d%H%M%S -d "-2 hours"`
endTime=`date +%Y%m%d%H%M%S -d "-1 hours"`
preDay=`date +%Y%m%d -d "-1 days"`
day=`date +%Y%m%d `
logFile=/data/log/user_category/processLog/llda/hourInf.log
echo "fiveMinInf $startTime" >> $logFile
textInputRoot=url_count/all_projects
#sh $baseDir/../Url_Count/bin/AccumulateUrlCount.sh $startTime $endTime >> $logFile
sh $baseDir/bin/accumulateUrlCount.sh $startTime $endTime >> $logFile
sh $baseDir/bin/crondInf.sh ${textInputRoot}/${startTime}_${endTime} >> $logFile
sh $baseDir/bin/etl.sh inf result  >> $logFile
