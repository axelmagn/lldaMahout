#!/bin/bash
baseDir=`dirname $0`/..
startDayCount=$1
endDayCount=$2
textInputRoot=url_count/all_projects
for((i=${startDayCount};i<${endDayCount};i++))do
  specialDay=`date +%Y%m%d -d "-${i} days"`
  hadoop fs -rm -r ${textInputRoot}/clean/${specialDay}*
  echo "sh $baseDir/bin/analysis.sh  wordClean  ${textInputRoot}/${specialDay}* ${textInputRoot}/clean/${specialDay}"
  sh $baseDir/bin/analysis.sh  wordClean  ${textInputRoot}/${specialDay}* ${textInputRoot}/clean/${specialDay}
done