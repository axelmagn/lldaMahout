#!/bin/bash
baseDir=`dirname $0`/..
oneDayAgo=`date +%Y%m%d -d "-1 days"`
twoDayAgo=`date +%Y%m%d -d "-2 days"`
day=`date +%Y%m%d `
rootPath=/user/hadoop/user_category/lldaMahout
resultRoot=user_category/lldaMahout/docTopics
localResultRoot=/data/log/user_category_result/pr
logFile=/data0/log/user_category/processLog/llda/dayEst.log
now=`date`
multiInput=${rootPath}/docs/to${twoDayAgo}:${rootPath}/docs/${oneDayAgo}/*
mergeOutput=${rootPath}/docs/to${oneDayAgo}
echo ${now} >> $logFile
sh ${baseDir}/bin/updateDict.sh  url_count/all_projects/clean/${oneDayAgo}*  >> $logFile 2>&1
sh ${baseDir}/bin/mergeLDocs.sh  ${multiInput} ${mergeOutput}   >> $logFile 2>&1
hadoop fs -rm -r ${rootPath}/tmpModels/*
sh ${baseDir}/bin/estDocs.sh ${mergeOutput}  >> $logFile  2>&1
sh $baseDir/bin/etl.sh ${resultRoot}/est ${resultRoot}/est_result ${localResultRoot}/total $startTime >> $logFile
