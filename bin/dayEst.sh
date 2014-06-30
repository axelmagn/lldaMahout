#!/bin/bash
baseDir=`dirname $0`/..
oneDayAgo=`date +%Y%m%d -d "-1 days"`
twoDayAgo=`date +%Y%m%d -d "-2 days"`
day=`date +%Y%m%d `
rootPath=/user/hadoop/user_category/lldaMahout
logFile=/data/log/user_category/processLog/llda/dayEst.log
now=`date`
multiInput=${rootPath}/docs/to${twoDayAgo}:${rootPath}/docs/${oneDayAgo}/*
mergeOutput=${rootPath}/docs/to${oneDayAgo}
echo ${now} >> $logFile
sh ${baseDir}/bin/updateDict.sh  url_count/all_projects/${oneDayAgo}*  >> $logFile 2>&1
sh ${baseDir}/bin/mergeLDocs.sh  ${multiInput} ${mergeOutput}   >> $logFile 2>&1
hadoop fs -rm -r ${rootPath}/tmpModels/*
sh ${baseDir}/bin/estDocs.sh ${mergeOutput}  >> $logFile  2>&1