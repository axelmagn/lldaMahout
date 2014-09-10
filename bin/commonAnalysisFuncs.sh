#!/bin/bash

function anaUserTrained(){
   rootPath=/user/hadoop/user_category/lldaMahout
   local logFile=/data0/log/user_category/processLog/llda/analysis.log

   local MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.UserTrainedDriver
   echo "hadoop jar $JAR $MAIN --input $1 --output $2 >> $logFile 2>&1"
   hadoop jar $JAR $MAIN --input $1 --output $2 >> $logFile 2>&1
}

function anaUser(){
   rootPath=/user/hadoop/user_category/lldaMahout
   local logFile=/data0/log/user_category/processLog/llda/analysis.log
   local MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.WordCountByUserDriver
   echo "hadoop jar $JAR $MAIN --input $1 --output $2 >> $logFile 2>&1"
   hadoop jar $JAR $MAIN --input $1 --output $2 >> $logFile 2>&1
}

function anaTopicModel(){
   rootPath=/user/hadoop/user_category/lldaMahout
   local logFile=/data0/log/user_category/processLog/llda/analysis.log
   local MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.TopicTermModelDriver
   echo "hadoop jar $JAR $MAIN --input $1 --output $2 >> $logFile 2>&1"
   hadoop jar $JAR $MAIN --input $1 --output $2 >> $logFile 2>&1
}

function anaUniqWord(){
   rootPath=/user/hadoop/user_category/lldaMahout
   local logFile=/data0/log/user_category/processLog/llda/analysis.log
   local MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.WordUniqDriver
   echo "hadoop jar $JAR $MAIN $1 ${rootPath}/analysis/tmp $2 >> $logFile 2>&1"
   hadoop jar $JAR $MAIN $1 ${rootPath}/analysis/tmp $2 >> $logFile 2>&1
}

function anaWordCount(){
   rootPath=/user/hadoop/user_category/lldaMahout
   local logFile=/data0/log/user_category/processLog/llda/analysis.log
   local MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.WordCountDriver
   echo "hadoop jar $JAR $MAIN --input $1 --output $2 >> $logFile 2>&1"
   hadoop jar $JAR $MAIN --input $1 --output $2 >> $logFile 2>&1
}

function anaLappedWord(){
   rootPath=/user/hadoop/user_category/lldaMahout
   local logFile=/data0/log/user_category/processLog/llda/analysis.log

   local MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.LappedWordDriver
   echo "hadoop jar $JAR $MAIN --input $1 --output $2 >> $logFile 2>&1"
   hadoop jar $JAR $MAIN --input $1 --output $2 >> $logFile 2>&1
}

function anaWordExtract(){
   rootPath=/user/hadoop/user_category/lldaMahout
   local logFile=/data0/log/user_category/processLog/llda/analysis.log

   local MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.WordExtractDriver
   echo "hadoop jar $JAR $MAIN --input $1 --output $2 >> $logFile 2>&1"
   hadoop jar $JAR $MAIN --input $1 --output $2 >> $logFile 2>&1
}



function anaHitWord(){
   rootPath=/user/hadoop/user_category/lldaMahout
   local logFile=/data0/log/user_category/processLog/llda/analysis.log

   local MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.HitWordCountDriver
   echo "hadoop jar $JAR $MAIN --input $1 --output $2 --resource_root ${rootPath}/resources >> $logFile 2>&1"
   hadoop jar $JAR $MAIN --input $1 --output $2 --resource_root ${rootPath}/resources >> $logFile 2>&1
}

function getWordHistory(){
   rootPath=/user/hadoop/user_category/lldaMahout
   local logFile=/data0/log/user_category/processLog/llda/analysis.log
   local MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.GetWordHistoryDriver
   echo "hadoop jar $JAR $MAIN --input $1 --output $2 --resource_root ${rootPath}/resources >> $logFile 2>&1"
   hadoop jar $JAR $MAIN --input $1 --output $2 --resource_root ${rootPath}/resources >> $logFile 2>&1
}

function anaWordLen(){
   rootPath=/user/hadoop/user_category/lldaMahout
   local logFile=/data0/log/user_category/processLog/llda/analysis.log

   local MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.WordLenDriver
   echo "hadoop jar $JAR $MAIN $1 $2 >> $logFile 2>&1"
   hadoop jar $JAR $MAIN $1 $2 >> $logFile 2>&1
}

