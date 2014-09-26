#!/bin/bash
function cleanWord(){
   rootPath=/user/hadoop/user_category/lldaMahout
   local logFile=/data0/log/user_category/processLog/llda/analysis.log

   local MAIN=com.elex.bigdata.llda.mahout.data.generatedocs.WordCleanDriver
   echo "hadoop jar $JAR $MAIN --input $1 --output $2 >> $logFile 2>&1"
   hadoop jar $JAR $MAIN --input $1 --output $2 >> $logFile 2>&1
}

function countUrl()
{

   local MAIN=com.elex.bigdata.llda.mahout.data.accumulate.Accumulate
   local logFile=/data0/log/user_category/processLog/llda/accumulate.log
   local outputBase=url_count/all_projects
   local content=url

   echo "hadoop jar $JAR  $MAIN --content $content --outputBase $outputBase --startTime $1 --endTime $2 >> $logFile 2>&1"
   hadoop jar $JAR  $MAIN --content $content --outputBase $outputBase --startTime $1 --endTime $2  >> $logFile 2>&1

   cleanWord ${outputBase}/$1_$2/* ${outputBase}/clean/$1_$2
}

function getNt()
{
   local MAIN=com.elex.bigdata.llda.mahout.data.accumulate.Accumulate
   local logFile=/data0/log/user_category/processLog/llda/accumulate.log
   local outputBase=user_category/lldaMahout/nations
   local content=nt

   echo "hadoop jar $JAR  $MAIN --content $content --outputBase $outputBase --startTime $1 --endTime $2 >> $logFile 2>&1"
   hadoop jar $JAR  $MAIN --content $content --outputBase $outputBase --startTime $1 --endTime $2  >> $logFile 2>&1
}

function countNt()
{
   local MAIN=com.elex.bigdata.llda.mahout.data.accumulate.Accumulate
   local logFile=/data0/log/user_nation/query.log
   local outputBase=user_attribute/nations
   local origoutput=user_category/lldaMahout/nations/
   local content=count

   local day=$1 ; local nextDay=`date +%Y%m%d -d "$day +1 days" ` ;local preDay=`date +%Y%m%d -d "$day -1 days" `
   local startTime=${day:0:8}000000; local endTime=${nextDay}000000

   echo "hadoop jar $JAR  $MAIN --content $content --outputBase $outputBase --startTime $startTime --endTime $endTime >> $logFile 2>&1"
   hadoop jar $JAR  $MAIN --content $content --outputBase $outputBase --startTime $startTime --endTime $endTime  >> $logFile 2>&1

   local local_path=/data1/user_attribute/nation
   for table in yac_user_action
   do
       transNtUid ${origoutput}/${startTime}_${endTime}/${table}/ ${outputBase}/trans/${day}/${table}/
       hadoop fs -getmerge ${outputBase}/trans/${day}/${table}/part* ${local_path}/${table}/${day}.log
   done

   for table in ad_all_log nav_all
   do
       transNtUid ${outputBase}/${startTime}_${endTime}/${table}/ ${outputBase}/trans/${day}/${table}/
       hadoop fs -getmerge ${outputBase}/trans/${day}/${table}/part* ${local_path}/${table}/${day}.log
   done

}

function batchGetNt(){
  origDay=$1;count=$2
  for((i=0;i<$count;i++));do
     startDay=`date +%Y%m%d -d "$origDay +${i} days"`
     ((j=i+1))
     endDay=`date +%Y%m%d -d "$origDay +${j} days" `
     getNt ${startDay}000000 ${endDay}000000
  done
}

function mergeNations()
{
  local MAIN=com.elex.bigdata.llda.mahout.data.accumulate.UniqMergeDriver
  local logFile=/data0/log/user_category/processLog/llda/mergeNations.log
  echo "hadoop jar $JAR $MAIN --multi_input $1 --output $2 >> $logFile 2>&1 "
  hadoop jar $JAR $MAIN --multi_input $1 --output $2 >> $logFile 2>&1
}

function transNtUid(){
  local MAIN=com.elex.bigdata.llda.mahout.data.transferUid.TransNtUidDriver
  local logFile=/data0/log/user_category/processLog/llda/transferNtUid.log
  echo "hadoop jar $JAR $MAIN --input $1 --output $2  >> $logFile 2>&1 "
  hadoop jar $JAR $MAIN --input $1 --output $2  >> $logFile 2>&1
}

function updateNtByDay(){
  local day=$1 ; local nextDay=`date +%Y%m%d -d "$day +1 days" ` ;local preDay=`date +%Y%m%d -d "$day -1 days" `
  local startTime=${day:0:8}000000; local endTime=${nextDay}000000
  getNt  $startTime $endTime
  local outputBase=user_category/lldaMahout/nations
  mergeNations ${outputBase}/${startTime}_${endTime}/*:${outputBase}/to${preDay} ${outputBase}/to${day:0:8}
  transNtUid  ${outputBase}/to${day:0:8} ${outputBase}/transTotal
}

function Error()
{
    echo "content error!!!  content should be url or nt " >> $logFile
}

function batchCountUrl(){
  local pattern=$1
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
   local pattern=$1
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
   local startDayCount=$1
   local endDayCount=$2
   local textInputRoot=url_count/all_projects
   for((i=${startDayCount};i<${endDayCount};i++))do
     specialDay=`date +%Y%m%d -d "-${i} days"`
     hadoop fs -rm -r ${textInputRoot}/clean/${specialDay}*
     echo "sh $baseDir/bin/analysis.sh  wordClean  ${textInputRoot}/${specialDay}* ${textInputRoot}/clean/${specialDay}"
     sh $baseDir/bin/analysis.sh  wordClean  ${textInputRoot}/${specialDay}* ${textInputRoot}/clean/${specialDay}
   done
}