#!/bin/bash

function anaInfResult(){
   resultRoot=/data0/log/user_category_result/pr
   rootPath=/user/hadoop/user_category/lldaMahout
   day=$1
   infResultDir=${resultRoot}/${day}
   infResultFile=${infResultDir}/analytics
   if test -e ${infResultFile}
     then
     rm ${infResultFile}
   fi
   cat ${infResultDir}/*  | awk '{print $1}'| sort | uniq | wc -l >> $infResultFile
   cat ${infResultDir}/*  | sort | uniq |  \
       awk '{sum[$2]+=1}END{for(key in sum){print key,sum[key]}}' | grep ^[0-9] >> $infResultFile
   cat $infResultFile | mail -s " inf category result ${day}" "yangbo@elex-tech.com"
}

function anaEstResult(){
  day=$1
  rootPath=/user/hadoop/user_category/lldaMahout
  resultRoot=/data0/log/user_category_result/pr/total
  estResultDir=${resultRoot}/${day}
  estResultFile=${estResultDir}/analytics
  cat ${estResultDir}/*  | awk '{print $1}'| wc -l >> $estResultFile
  cat ${estResultDir}/*  |   \
          awk '{sum[$2]+=1}END{for(key in sum){print key,sum[key]}}' | grep ^[0-9] >> $estResultFile
  cat ${estResultFile} | mail -s " est category analytics to${day}" "yangbo@elex-tech.com"
  hadoop fs -rm ${rootPath}/analysis/category/*
  hadoop fs -copyFromLocal  ${estResultDir}/0.0  ${rootPath}/analysis/category/userCategory
  hadoop fs -copyFromLocal  ${estResultFile} ${rootPath}/analysis/category/analytics
}

function anaCategoryDist(){
  rootPath=/user/hadoop/user_category/lldaMahout
  MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.CategoryAnalyzeTool
  logFile=/data0/log/user_category/processLog/llda/analysis/categoryDist.log
  echo "hadoop jar $JAR $MAIN --input $1 --output $2 --category_result_dir ${rootPath}/analysis/category >> $logFile 2>&1"
  hadoop jar $JAR $MAIN --input $1 --output $2 --category_result_dir ${rootPath}/analysis/category >> $logFile 2>&1
}

function updateAnaCategoryDist(){
  day=$1
  nationDir=user_category/lldaMahout/nations
  categoryAnaDir=user_category/lldaMahout/analysis/category
  resultRoot=/data0/log/user_category_result/pr/total
  source $baseDir/bin/accumulateFuncs.sh
  updateNtByDay $day
  anaEstResult $day
  anaCategoryDist ${nationDir}/transTotal ${categoryAnaDir}/dist
  hadoop fs -cat ${categoryAnaDir}/categoryDist/* > ${resultRoot}/${day}/categoryDist
  cat ${resultRoot}/${day}/categoryDist | mail -s " category comp result ${day}" "yangbo@elex-tech.com"
  preDay=`date +%Y%m%d -d "$day -1 days"`
  #compareCategoryDist $day  $preDay

}

function compareCategoryDist(){
  day=$1;anoDay=$2
  resultRoot=/data0/log/user_category_result/pr/total
  file1=${resultRoot}/${day}/categoryDist; file2=${resultRoot}/${anoDay}/categoryDist
  resultFile=${resultRoot}/${day}/categoryDistComp
  python $baseDir/bin/compAnaResult.py $file1 $file2 > $resultFile
  cat $resultFile | mail -s " category comp result ${day}" "yangbo@elex-tech.com"
}


