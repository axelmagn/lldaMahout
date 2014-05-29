#!/bin/bash
baseDir=`dirname $0`/..
startTime=`date +%Y%m%d%H%M%S -d "-10 mins"`
endTime=`date +%Y%m%d%H%M%S -d "-5 mins"`
preDay=`date +%Y%m%d -d "-1 days"`
day=`date +%Y%m%d `
sh $baseDir/../Url_Count/bin/AccumulateUrlCount.sh $startTime $endTime
sh $baseDir/bin/updateDict.sh url_count/*/${startTime}_${endTime}
sh $baseDir/bin/getInfDocs.sh url_count/*/${day}* to${preDay} ${day}
sh $baseDir/bin/infDocs.sh ${day}
sh $baseDir/bin/etl.sh inf result