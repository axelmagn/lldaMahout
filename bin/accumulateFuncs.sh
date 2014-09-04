#!/bin/bash
function cleanWord(){
   rootPath=/user/hadoop/user_category/lldaMahout
   logFile=/data0/log/user_category/processLog/llda/analysis.log

   MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.WordCleanDriver
   echo "hadoop jar $JAR $MAIN --input $1 --output $2 >> $logFile 2>&1"
   hadoop jar $JAR $MAIN --input $1 --output $2 >> $logFile 2>&1
}

function countUrl()
{

   MAIN=com.elex.bigdata.llda.mahout.data.accumulate.Accumulate
   logFile=/data0/log/user_category/processLog/llda/accumulate.log
   outputBase=url_count/all_projects
   content=url

   echo "hadoop jar $JAR  $MAIN --content $content --outputBase $outputBase --startTime $1 --endTime $2 >> $logFile 2>&1"
   hadoop jar $JAR  $MAIN --content $content --outputBase $outputBase --startTime $1 --endTime $2  >> $logFile 2>&1

   cleanWord ${outputBase}/$1_$2 ${outputBase}/clean/$1_$2
}

function getNt()
{
   MAIN=com.elex.bigdata.llda.mahout.data.accumulate.Accumulate
   logFile=/data0/log/user_category/processLog/llda/accumulate.log
   outputBase=user_category/lldaMahout/nations
   content=nt

   echo "hadoop jar $JAR  $MAIN --content $content --outputBase $outputBase --startTime $1 --endTime $2 >> $logFile 2>&1"
   hadoop jar $JAR  $MAIN --content $content --outputBase $outputBase --startTime $1 --endTime $2  >> $logFile 2>&1
}

function mergeNations()
{
  MAIN=com.elex.bigdata.llda.mahout.data.UniqMergeDriver
  hadoop jar $JAR --multi_input $1 --output $2 >> $logFile 2>&1 &
  echo "hadoop jar $JAR --multi_input $1 --output $2 >> $logFile 2>&1 &"
}

function transNtUid(){
  MAIN=com.elex.bigdata.llda.mahout.data.transferUid.TransNtUidDriver
  logFile=/data0/log/user_category/processLog/llda/transferNtUid.log
  echo "hadoop jar $JAR $MAIN --input $1 --output $2  >> $logFile 2>&1 "
  hadoop jar $JAR $MAIN --input $1 --output $2  >> $logFile 2>&1
}

function updateNtByDay(){
  day=$1 ; nextDay=`date +%Y%m%d -d "$day +1 days" ` ;preDay=`date +%Y%m%d -d "$day -1 days" `
  startTime=${day:0:8}000000; endTime=${nextDay}000000
  getNt  $startTime $endTime
  outputBase=user_category/lldaMahout/nations
  mergetNations ${outputBase}/${startTime}_${endTime}:${outputBase}/to${preDay} ${outputBase}/to${day:0:8}
  transNtUid  ${outputBase}/to${day:0:8} ${outputBase}/transTotal
}

function Error()
{
    echo "content error!!!  content should be url or nt " >> $logFile
}

function batchCountUrl(){
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
    countUrl ${startTime} ${endTime}
  done
}

function batchCleanWord(){
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
     cleanWord url_count/all_projects/${startTime}_${endTime}/*  url_count/all_projects/clean/${startTime}_${endTime}
   done
}

function reCleanWord(){
   startDayCount=$1
   endDayCount=$2
   textInputRoot=url_count/all_projects
   for((i=${startDayCount};i<${endDayCount};i++))do
     specialDay=`date +%Y%m%d -d "-${i} days"`
     hadoop fs -rm -r ${textInputRoot}/clean/${specialDay}*
     echo "sh $baseDir/bin/analysis.sh  wordClean  ${textInputRoot}/${specialDay}* ${textInputRoot}/clean/${specialDay}"
     sh $baseDir/bin/analysis.sh  wordClean  ${textInputRoot}/${specialDay}* ${textInputRoot}/clean/${specialDay}
   done
}