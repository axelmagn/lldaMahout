#!/bin/bash
baseDir=`dirname $0`/..
oneDayAgo=`date +%Y%m%d -d "-1 days"`
twoDayAgo=`date +%Y%m%d -d "-2 days"`
day=`date +%Y%m%d `
rootPath=/user/hadoop/user_category/lldaMahout
multiInput=${rootPath}/docs/to${twoDayAgo}:${rootPath}/docs/${oneDayAgo}
mergeOutput=${rootPath}/docs/to${oneDayAgo}
sh ${baseDir}/bin/updateDict.sh  ${rootPath}/docs/${oneDayAgo}
sh ${baseDir}/bin/mergeLDocs.sh  ${multiInput} ${mergeOutput}
hadoop fs -rm -r ${rootPath}/tmpModels/*
sh ${baseDir}/bin/estDocs.sh ${mergeOutput}