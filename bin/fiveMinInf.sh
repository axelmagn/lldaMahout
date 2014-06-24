#!/bin/bash
baseDir=`dirname $0`/..
startTime=`date +%Y%m%d%H%M%S -d "-10 mins"`
endTime=`date +%Y%m%d%H%M%S -d "-5 mins"`
preDay=`date +%Y%m%d -d "-1 days"`
day=`date +%Y%m%d `
logFile=/data/log/user_category/processLog/llda/hadoopLLDA.log
echo "fiveMinInf $startTime" >> $logFile
#sh $baseDir/../Url_Count/bin/AccumulateUrlCount.sh $startTime $endTime >> $logFile
sh $baseDir/bin/accumulateUrlCount.sh $startTime $endTime >> $logFile
sh $baseDir/bin/crondInf.sh $startTime_$endTime
sh $baseDir/bin/etl.sh inf result  >> $logFile
