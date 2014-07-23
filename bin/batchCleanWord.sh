#!/bin/bash
baseDir=`dirname $0`/..
day=`date +%Y%m%d `
pattern=$1
echo $pattern
echo " hadoop fs -ls url_count/all_projects/ | grep ${pattern} "
files=`hadoop fs -ls url_count/all_projects/ | grep ${pattern} | tr -s " " " " | cut -f8 -d" "`
echo ${files[@]}
for file in ${files[@]};do
  time=${file##*/}
  startTime=${time%%_*}
  endTime=${time##*_}
  echo $startTime $endTime
  sh $baseDir/bin/analysis.sh  url_count/all_projects/${startTime}_${endTime}  url_count/all_projects/clean/${startTime}_${endTime}
done