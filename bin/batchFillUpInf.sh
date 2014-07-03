#!/bin/bash
baseDir=`dirname $0`/..
day=`date +%Y%m%d `
pattern=$1
echo $pattern
echo " hadoop fs -ls url_count/all_projects/ | grep ${pattern} "
files=`hadoop fs -ls url_count/all_projects/ | grep ${pattern} | tr -s " " " " | cut -f8 -d" "`
for file in ${files[@]};do
  startTime=${files##*/}
  startTime=${startTime%%_*}
  echo $startTime
  sh $baseDir/bin/crondInf.sh ${file}
  sh $baseDir/bin/etl.sh inf result ${startTime}
done