#!/bin/bash

function genDocs(){
   MAIN=com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver
   rootPath=/user/hadoop/user_category/lldaMahout
   inputPath=$1
   outputPath=$2
   logFile=/data0/log/user_category/processLog/llda/genDocs.log
   echo "hadoop jar $JAR $MAIN  --dict_root ${rootPath}/dictionary  --resource_root ${rootPath}/resources \
         --input $inputPath --output $outputPath  >> $logFile 2>&1"
   hadoop jar $JAR $MAIN  --dict_root ${rootPath}/dictionary  --resource_root ${rootPath}/resources \
   --input $inputPath --output $outputPath  >> $logFile 2>&1
}

function genDocsAndUids(){
   MAIN=com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver
   rootPath=/user/hadoop/user_category/lldaMahout
   inputPath=$1
   uidPath=$3
   outputPath=$2
   logFile=/data0/log/user_category/processLog/llda/genDocsAndUids.log
   echo "hadoop jar $JAR $MAIN  --dict_root ${rootPath}/dictionary  --resource_root ${rootPath}/resources \
      --input $inputPath --output $outputPath --uid_path $uidPath >> $logFile 2>&1"
   hadoop jar $JAR $MAIN  --dict_root ${rootPath}/dictionary  --resource_root ${rootPath}/resources \
   --input $inputPath --output $outputPath --uid_path $uidPath >> $logFile 2>&1
}

function reGenDocsByDay(){
  rootPath=/user/hadoop/user_category/lldaMahout
  textInputRoot=url_count/all_projects
  dayCount=$1
  for((i=1;i<${dayCount};i++))do
    specialDay=`date +%Y%m%d -d "-${i} days"`
    echo "genDocs  ${textInputRoot}/clean/${specialDay}* ${rootPath}/docs/${specialDay}"
    genDocs  ${textInputRoot}/clean/${specialDay}* ${rootPath}/docs/${specialDay} &
  done
}

function mergeDocs(){
  MAIN=com.elex.bigdata.llda.mahout.data.mergedocs.MergeLDocDriver
  multiInput=$1
  output=$2
  logFile=/data0/log/user_category/processLog/llda/mergeDocs.log
  echo `date` >> $logFile
  echo "hadoop jar $JAR $MAIN --multi_input $multiInput --output $output >> $logFile 2>&1"
  hadoop jar $JAR $MAIN --multi_input $multiInput --output $output >> $logFile 2>&1
}

function compDocs(){
  MAIN=com.elex.bigdata.llda.mahout.data.mergedocs.MergeLDocDriver
  multiInput=$1
  output=$2
  uidFilePath=$3
  logFile=/data0/log/user_category/processLog/llda/compDocs.log
  echo `date` >> $logFile
  echo "hadoop jar $JAR $MAIN --multi_input $multiInput --output $output --uid_path $uidFilePath --use_cookieId cookie >> $logFile 2>&1"
  hadoop jar $JAR $MAIN --multi_input $multiInput --output $output --uid_path $uidFilePath --use_cookieId cookie >> $logFile 2>&1
}

function transDocUid(){
  MAIN=com.elex.bigdata.llda.mahout.data.transferUid.TransDocUidDriver
  logFile=/data0/log/user_category/processLog/llda/transferUid.log
  echo "hadoop jar $JAR $MAIN --input $1 --output $2  >> $logFile 2>&1 "
  hadoop jar $JAR $MAIN --input $1 --output $2  >> $logFile 2>&1
}

function infDocs(){
  MAIN=com.elex.bigdata.llda.mahout.mapreduce.inf.LLDAInfDriver
  rootPath=/user/hadoop/user_category/lldaMahout
  echo "hadoop jar $JAR $MAIN  --input ${rootPath}/docs/$1 -k 42 --output ${rootPath}/models --maxIter 40 -mipd 1 --dictionary ${rootPath}/dictionary/dict  --resource_root ${rootPath}/resources \
      -dt ${rootPath}/docTopics/inf -mt ${rootPath}/tmpModels --num_reduce_tasks 1 --num_train_threads 8 --num_update_threads 4 --test_set_fraction 0.1 --iteration_block_size 4"
  hadoop jar $JAR $MAIN  --input ${rootPath}/docs/$1 -k 42 --output ${rootPath}/models --maxIter 40 -mipd 1 --dictionary ${rootPath}/dictionary/dict --resource_root ${rootPath}/resources  \
  -dt ${rootPath}/docTopics/inf -mt ${rootPath}/tmpModels --num_train_threads 8 --num_update_threads 4 --test_set_fraction 0.1 --iteration_block_size 4
}

function crondInf(){
  MAIN=com.elex.bigdata.llda.mahout.crond.CrondInfDriver
  rootPath=/user/hadoop/user_category/lldaMahout
  inputPath=$1
  logFile=/data0/log/user_category/processLog/llda/crondInf.log
  now=`date`
  echo ${now} >> $logFile
  echo "hadoop jar $JAR $MAIN --input $inputPath --doc_root ${rootPath}/docs --dict_root ${rootPath}/dictionary \
      --resource_root ${rootPath}/resources --num_topics 42 --model_input ${rootPath}/models --output ${rootPath}/docTopics/inf >> $logFile 2>&1"
  hadoop jar $JAR $MAIN --input $inputPath --doc_root ${rootPath}/docs --dict_root ${rootPath}/dictionary \
  --resource_root ${rootPath}/resources --num_topics 42 --model_input ${rootPath}/models --output ${rootPath}/docTopics/inf  >> $logFile 2>&1
}

function etl(){
  MAIN=com.elex.bigdata.llda.mahout.mapreduce.etl.ResultEtlDriver
  rootPath=/user/hadoop/user_category/lldaMahout
  logFile=/data0/log/user_category/processLog/llda/etl.log
  if [ $# -lt 4 ];then
    echo "hadoop jar $JAR $MAIN --input $1 --output $2 --local_result_root $3 --resource_root ${rootPath}/resources >> $logFile 2>&1"
    hadoop jar $JAR $MAIN --input $1 --output $2 --local_result_root $3 --resource_root ${rootPath}/resources >> $logFile 2>&1
  else
    echo "hadoop jar $JAR $MAIN --input $1 --output $2 --local_result_root $3 --result_time $4 --resource_root ${rootPath}/resources >> $logFile 2>&1"
    hadoop jar $JAR $MAIN --input $1 --output $2 --local_result_root $3 --result_time $4 --resource_root ${rootPath}/resources >> $logFile 2>&1
  fi
}

function batchFillUpInf(){
  pattern=$1
  echo $pattern
  resultRoot=user_category/lldaMahout/docTopics
  localResultRoot=/data0/log/user_category_result/pr/
  echo " hadoop fs -ls url_count/all_projects/clean | grep ${pattern} "
  files=`hadoop fs -ls url_count/all_projects/clean | grep ${pattern} | tr -s " " " " | cut -f8 -d" "`
  echo ${files[@]}
  for file in ${files[@]};do
    startTime=${file##*/}
    startTime=${startTime%%_*}
    echo $startTime
    crondInf  ${file}
    etl ${resultRoot}/inf ${resultRoot}/inf_result ${localResultRoot} $startTime
  done
}

function infOrigData(){
  logFile=/data0/log/user_category/processLog/llda/infOrigData.log
  textInputRoot=url_count/all_projects
  resultRoot=user_category/lldaMahout/docTopics
  localResultRoot=/data0/log/user_category_result/pr
  now=`date`
  echo $now >> $logFile
  source $baseDir/bin/accumulateFuncs.sh
  startTime=$1 ; endTime=$2
  countUrl $startTime $endTime
  crondInf ${textInputRoot}/clean/${startTime}_${endTime}
  etl ${resultRoot}/inf ${resultRoot}/inf_result ${localResultRoot} ${startTime:0:10}0000
}

function estDocs(){
  MAIN=com.elex.bigdata.llda.mahout.mapreduce.est.LLDADriver
  rootPath=/user/hadoop/user_category/lldaMahout
  inputDocs=$1
  logFile=/data0/log/user_category/processLog/llda/est.log
  now=`date`
  echo $now >> $logFile
  echo "hadoop jar $JAR $MAIN  --input $inputDocs -k 42 --output ${rootPath}/models --maxIter 40 -mipd 1 --dictionary ${rootPath}/dictionary/dict --resource_root ${rootPath}/resources \
      -dt ${rootPath}/docTopics/est -mt ${rootPath}/tmpModels --num_reduce_tasks 5 --num_train_threads 8 --num_update_threads 4 --test_set_fraction 0.1 --iteration_block_size 3 >> $logFile 2>&1"
  hadoop jar $JAR $MAIN  --input $inputDocs -k 42 --output ${rootPath}/models --maxIter 40 -mipd 1 --dictionary ${rootPath}/dictionary/dict --resource_root ${rootPath}/resources \
  -dt ${rootPath}/docTopics/est -mt ${rootPath}/tmpModels --num_reduce_tasks 5 --num_train_threads 8 --num_update_threads 4 --test_set_fraction 0.1 --iteration_block_size 3 >> $logFile 2>&1

}

function updateDict(){
  MAIN=com.elex.bigdata.llda.mahout.dictionary.UpdateDictDriver
  rootPath=/user/hadoop/user_category/lldaMahout
  logFile=/data0/log/user_category/processLog/llda/updateDict.log
  echo `date` >> $logFile
  if [ $# -ge 3 ];then
    echo "hadoop jar $JAR $MAIN --input $1 --count_threshold $2 --count_upper_threshold $3 --dict_root ${rootPath}/dictionary --resource_root ${rootPath}/resources >> $logFile 2>&1"
    hadoop jar $JAR $MAIN --input $1 --count_threshold $2 --count_upper_threshold $3 --dict_root ${rootPath}/dictionary --resource_root ${rootPath}/resources  >> $logFile 2>&1
  elif [ $# -ge 2 ];then
    echo "hadoop jar $JAR $MAIN --input $1 --count_threshold $2  --dict_root ${rootPath}/dictionary --resource_root ${rootPath}/resources >> $logFile 2>&1"
    hadoop jar $JAR $MAIN --input $1 --count_threshold $2  --dict_root ${rootPath}/dictionary --resource_root ${rootPath}/resources >> $logFile 2>&1
  else
    echo "hadoop jar $JAR $MAIN --input $1  --dict_root ${rootPath}/dictionary --resource_root ${rootPath}/resources >> $logFile 2>&1"
    hadoop jar $JAR $MAIN --input $1  --dict_root ${rootPath}/dictionary --resource_root ${rootPath}/resources  >> $logFile 2>&1
  fi
}

function updateEstByDay(){
  day=$1
  oneDayAgo=`date +%Y%m%d -d " $day -1 days"`
  rootPath=/user/hadoop/user_category/lldaMahout
  resultRoot=user_category/lldaMahout/docTopics
  localResultRoot=/data0/log/user_category_result/pr
  logFile=/data0/log/user_category/processLog/llda/dayEst.log

  multiInput=${rootPath}/docs/to${oneDayAgo}:${rootPath}/docs/${day}/*
  mergeOutput=${rootPath}/docs/to${day}
  estInput=${rootPath}/docs/est

  now=`date +%Y%m%d`
  echo "---------------------------------------------------------------------------------" >> $logFile
  echo ${now} >> $logFile

  updateDict  url_count/all_projects/clean/${day}*
  mergeDocs  ${multiInput} ${mergeOutput}
  transDocUid ${mergeOutput} ${estInput}
  hadoop fs -rm -r ${rootPath}/tmpModels/*
  estDocs ${estInput}
  etl ${resultRoot}/est ${resultRoot}/est_result ${localResultRoot}/total ${day}000000

}

