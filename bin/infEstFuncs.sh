#!/bin/bash

function genDocs(){
   local MAIN=com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver
   local rootPath=/user/hadoop/user_category/lldaMahout
   local inputPath=$1
   local outputPath=$2
   local logFile=/data0/log/user_category/processLog/llda/genDocs.log
   echo "hadoop jar $JAR $MAIN  --dict_root ${rootPath}/dictionary  --resource_root ${rootPath}/resources \
         --input $inputPath --output $outputPath  >> $logFile 2>&1"
   hadoop jar $JAR $MAIN  --dict_root ${rootPath}/dictionary  --resource_root ${rootPath}/resources \
   --input $inputPath --output $outputPath  >> $logFile 2>&1
}

function genDocsAndUids(){
   local MAIN=com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver
   local rootPath=/user/hadoop/user_category/lldaMahout
   local inputPath=$1
   local uidPath=$3
   local outputPath=$2
   local logFile=/data0/log/user_category/processLog/llda/genDocsAndUids.log
   echo "hadoop jar $JAR $MAIN  --dict_root ${rootPath}/dictionary  --resource_root ${rootPath}/resources \
      --input $inputPath --output $outputPath --uid_path $uidPath >> $logFile 2>&1"
   hadoop jar $JAR $MAIN  --dict_root ${rootPath}/dictionary  --resource_root ${rootPath}/resources \
   --input $inputPath --output $outputPath --uid_path $uidPath >> $logFile 2>&1
}

function reGenDocsByDay(){
  local rootPath=/user/hadoop/user_category/lldaMahout
  local textInputRoot=url_count/all_projects
  local dayCount=$1
  for((i=1;i<${dayCount};i++))do
    specialDay=`date +%Y%m%d -d "-${i} days"`
    echo "genDocs  ${textInputRoot}/clean/${specialDay}* ${rootPath}/docs/${specialDay}"
    genDocs  ${textInputRoot}/clean/${specialDay}* ${rootPath}/docs/${specialDay} &
  done
}

function mergeDocs(){
  local MAIN=com.elex.bigdata.llda.mahout.data.mergedocs.MergeLDocDriver
  local multiInput=$1
  local output=$2
  local logFile=/data0/log/user_category/processLog/llda/mergeDocs.log
  echo `date` >> $logFile
  echo "hadoop jar $JAR $MAIN --multi_input $multiInput --output $output >> $logFile 2>&1"
  hadoop jar $JAR $MAIN --multi_input $multiInput --output $output >> $logFile 2>&1
}

function compDocs(){
  local MAIN=com.elex.bigdata.llda.mahout.data.mergedocs.MergeLDocDriver
  local multiInput=$1
  local output=$2
  local uidFilePath=$3
  local logFile=/data0/log/user_category/processLog/llda/compDocs.log
  echo `date` >> $logFile
  echo "hadoop jar $JAR $MAIN --multi_input $multiInput --output $output --uid_path $uidFilePath --use_cookieId cookie >> $logFile 2>&1"
  hadoop jar $JAR $MAIN --multi_input $multiInput --output $output --uid_path $uidFilePath --use_cookieId cookie >> $logFile 2>&1
}

function transDocUid(){
  local MAIN=com.elex.bigdata.llda.mahout.data.transferUid.TransDocUidDriver
  local logFile=/data0/log/user_category/processLog/llda/transferUid.log
  echo "hadoop jar $JAR $MAIN --input $1 --output $2  >> $logFile 2>&1 "
  hadoop jar $JAR $MAIN --input $1 --output $2  >> $logFile 2>&1
}

function infDocs(){
  local MAIN=com.elex.bigdata.llda.mahout.mapreduce.inf.LLDAInfDriver
  local rootPath=/user/hadoop/user_category/lldaMahout
  echo "hadoop jar $JAR $MAIN  --input ${rootPath}/docs/$1 -k 42 --output ${rootPath}/models --maxIter 40 -mipd 1 --dictionary ${rootPath}/dictionary/dict  --resource_root ${rootPath}/resources \
      -dt ${rootPath}/docTopics/inf -mt ${rootPath}/tmpModels --num_reduce_tasks 1 --num_train_threads 8 --num_update_threads 4 --test_set_fraction 0.1 --iteration_block_size 4"
  hadoop jar $JAR $MAIN  --input ${rootPath}/docs/$1 -k 42 --output ${rootPath}/models --maxIter 40 -mipd 1 --dictionary ${rootPath}/dictionary/dict --resource_root ${rootPath}/resources  \
  -dt ${rootPath}/docTopics/inf -mt ${rootPath}/tmpModels --num_train_threads 8 --num_update_threads 4 --test_set_fraction 0.1 --iteration_block_size 4
}

function crondInf(){
  local MAIN=com.elex.bigdata.llda.mahout.crond.CrondInfDriver
  local rootPath=/user/hadoop/user_category/lldaMahout
  local inputPath=$1
  local logFile=/data0/log/user_category/processLog/llda/crondInf.log
  local now=`date`
  echo ${now} >> $logFile
  echo "hadoop jar $JAR $MAIN --input $inputPath --doc_root ${rootPath}/docs --dict_root ${rootPath}/dictionary \
      --resource_root ${rootPath}/resources --num_topics 42 --model_input ${rootPath}/models --output ${rootPath}/docTopics/inf >> $logFile 2>&1"
  hadoop jar $JAR $MAIN --input $inputPath --doc_root ${rootPath}/docs --dict_root ${rootPath}/dictionary \
  --resource_root ${rootPath}/resources --num_topics 42 --model_input ${rootPath}/models --output ${rootPath}/docTopics/inf  >> $logFile 2>&1
}

function etl(){
  local MAIN=com.elex.bigdata.llda.mahout.mapreduce.etl.ResultEtlDriver
  local rootPath=/user/hadoop/user_category/lldaMahout
  local logFile=/data0/log/user_category/processLog/llda/etl.log
  if [ $# -lt 4 ];then
    echo "hadoop jar $JAR $MAIN --input $1 --output $2 --local_result_root $3 --resource_root ${rootPath}/resources >> $logFile 2>&1"
    hadoop jar $JAR $MAIN --input $1 --output $2 --local_result_root $3 --resource_root ${rootPath}/resources >> $logFile 2>&1
  else
    echo "hadoop jar $JAR $MAIN --input $1 --output $2 --local_result_root $3 --result_time $4 --resource_root ${rootPath}/resources >> $logFile 2>&1"
    hadoop jar $JAR $MAIN --input $1 --output $2 --local_result_root $3 --result_time $4 --resource_root ${rootPath}/resources >> $logFile 2>&1
  fi
}

function batchFillUpInf(){
  local pattern=$1
  echo $pattern
  local resultRoot=user_category/lldaMahout/docTopics
  local localResultRoot=/data0/log/user_category_result/pr/
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
  local logFile=/data0/log/user_category/processLog/llda/infOrigData.log
  local textInputRoot=url_count/all_projects
  local resultRoot=user_category/lldaMahout/docTopics
  local localResultRoot=/data0/log/user_category_result/pr
  local now=`date`
  echo $now >> $logFile
  source ${baseDir}/bin/accumulateFuncs.sh
  local startTime=$1 ; local endTime=$2
  countUrl $startTime $endTime
  crondInf ${textInputRoot}/clean/${startTime}_${endTime}
  etl ${resultRoot}/inf ${resultRoot}/inf_result ${localResultRoot} ${startTime:0:10}0000
}

function estDocs(){
  local MAIN=com.elex.bigdata.llda.mahout.mapreduce.est.LLDADriver
  local rootPath=/user/hadoop/user_category/lldaMahout
  local inputDocs=$1
  local logFile=/data0/log/user_category/processLog/llda/est.log
  local now=`date`
  echo $now >> $logFile
  echo "hadoop jar $JAR $MAIN  --input $inputDocs -k 42 --output ${rootPath}/models --maxIter 40 -mipd 1 --dictionary ${rootPath}/dictionary/dict --resource_root ${rootPath}/resources \
      -dt ${rootPath}/docTopics/est -mt ${rootPath}/tmpModels --num_reduce_tasks 5 --num_train_threads 8 --num_update_threads 4 --test_set_fraction 0.1 --iteration_block_size 3 >> $logFile 2>&1"
  hadoop jar $JAR $MAIN  --input $inputDocs -k 42 --output ${rootPath}/models --maxIter 40 -mipd 1 --dictionary ${rootPath}/dictionary/dict --resource_root ${rootPath}/resources \
  -dt ${rootPath}/docTopics/est -mt ${rootPath}/tmpModels --num_reduce_tasks 5 --num_train_threads 8 --num_update_threads 4 --test_set_fraction 0.1 --iteration_block_size 3 >> $logFile 2>&1

}

function updateDict(){
  local MAIN=com.elex.bigdata.llda.mahout.dictionary.UpdateDictDriver
  local rootPath=/user/hadoop/user_category/lldaMahout
  local logFile=/data0/log/user_category/processLog/llda/updateDict.log
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

function getDictWord(){
   rootPath=/user/hadoop/user_category/lldaMahout
   local logFile=/data0/log/user_category/processLog/llda/getDictWord.log

   local MAIN=com.elex.bigdata.llda.mahout.dictionary.GetDictWordDriver
   echo "hadoop jar $JAR $MAIN --input $1 --output $2 >> $logFile 2>&1"
   hadoop jar $JAR $MAIN --input $1 --output $2 >> $logFile 2>&1
}

function updateEstByDay(){
  local day=$1
  local preDay=`date +%Y%m%d -d " $day -1 days"`
  local rootPath=/user/hadoop/user_category/lldaMahout
  local resultRoot=user_category/lldaMahout/docTopics
  local localResultRoot=/data0/log/user_category_result/pr
  local logFile=/data0/log/user_category/processLog/llda/dayEst.log

  local multiInput=${rootPath}/docs/to${preDay}:${rootPath}/docs/${day}/*
  local mergeOutput=${rootPath}/docs/to${day}
  local estInput=${rootPath}/docs/est

  updateDict  url_count/all_projects/clean/${day}*
  mergeDocs  ${multiInput} ${mergeOutput}
  transDocUid ${mergeOutput} ${estInput}
  hadoop fs -rm -r ${rootPath}/tmpModels/*
  estDocs ${estInput}
  etl ${resultRoot}/est ${resultRoot}/est_result ${localResultRoot}/total ${day}000000

}

function batchFillUpEst(){
  local pattern=$1
  echo $pattern
  local resultRoot=user_category/lldaMahout/docTopics
  local localResultRoot=/data0/log/user_category_result/pr
  echo " hadoop fs -ls user_category/lldaMahout/docs/ | grep to${pattern} "
  files=`hadoop fs -ls user_category/lldaMahout/docs | grep to${pattern} | tr -s " " " " | cut -f8 -d" "`
  echo ${files[@]}
  local estInput=${rootPath}/docs/est
  for file in ${files[@]};do
    echo "estDocs $file"
    transDocUid $file ${estInput}
    estDocs $estInput
    echo "etl ${resultRoot}/est ${resultRoot}/est_result ${localResultRoot}/total ${file##*to}000000"
    etl ${resultRoot}/est ${resultRoot}/est_result ${localResultRoot}/total ${file##*to}000000
  done
}

