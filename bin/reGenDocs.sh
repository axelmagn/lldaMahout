#!/bin/bash
baseDir=`dirname $0`/..
oneDayAgo=`date +%Y%m%d -d "-1 days"`
twoDayAgo=`date +%Y%m%d -d "-2 days"`
day=`date +%Y%m%d `
rootPath=/user/hadoop/user_category/lldaMahout
textInputRoot=url_count/all_projects
dayCount=$1
for((index=1;index<${dayCount};index++))do
  specialDay=`date +%Y%m%d -d "-${index} days"`
  echo "sh ${baseDir}/bin/genDocs.sh  ${textInputRoot}/${specialDay}* ${rootPath}/docs/${specialDay}"
  sh ${baseDir}/bin/genDocs.sh  ${textInputRoot}/${specialDay}* ${rootPath}/${specialDay} &
done