#!/bin/bash
baseDir=`dirname $0`/..
dayCount=$1
textInputRoot=url_count/all_projects
for((i=1;i<${dayCount};i++))do
  specialDay=`date +%Y%m%d -d "-${i} days"`
  hadoop fs -rm -r ${textInputRoot}/clean/${specialDay}*
  echo "sh $baseDir/bin/analysis.sh  wordClean  ${textInputRoot}/$specialDay}* ${textInputRoot}/clean/${speicialDay}"
  sh $baseDir/bin/analysis.sh  wordClean  ${textInputRoot}/${specialDay}* ${textInputRoot}/clean/${speicialDay}
done