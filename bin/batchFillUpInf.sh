#!/bin/bash
baseDir=`dirname $0`/..
day=`date +%Y%m%d `
pattern=$1
echo $pattern
resultRoot=user_category/lldaMahout/docTopics
localResultRoot=/data/log/user_category_result/pr
echo " hadoop fs -ls url_count/all_projects/clean | grep ${pattern} "
files=`hadoop fs -ls url_count/all_projects/clean | grep ${pattern} | tr -s " " " " | cut -f8 -d" "`
echo ${files[@]}
for file in ${files[@]};do
  startTime=${file##*/}
  startTime=${startTime%%_*}
  echo $startTime
  sh $baseDir/bin/crondInf.sh ${file}
  sh $baseDir/bin/etl.sh ${resultRoot}/inf ${resultRoot}/inf_result ${localResultRoot} $startTime >> $logFile
done