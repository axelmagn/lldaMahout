#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
logFile=/data0/log/user_category/processLog/llda/dayEst.log
oneDayAgo=`date +%Y%m%d -d "-1 days"`
for((i=0;i<5;i++))do
  echo " ">>$logFile
done
echo "-----------------------------------------------------------------------" >> $logFile
echo "                   $oneDayAgo                                          " >> $logFile
echo "-----------------------------------------------------------------------" >> $logFile
source $baseDir/bin/infEstFuncs.sh
echo "                   updateEstByDay $oneDayAgo                     " >> $logFile
updateEstByDay $oneDayAgo
source $baseDir/bin/categoryAnalysisFuncs.sh
echo "                   anaInfResult $oneDayAgo                       " >> $logFile
anaInfResult $oneDayAgo
echo "                   updateAnaCategoryDist $oneDayAgo              " >> $logFile
updateAnaCategoryDist $oneDayAgo


