#!/bin/bash

function anaInfResult(){
   resultRoot=/data0/log/user_category_result/pr
   rootPath=/user/hadoop/user_category/lldaMahout
   local day=$1
   local infResultDir=${resultRoot}/${day}
   local infResultFile=${infResultDir}/analytics
   if test -e ${infResultFile}
     then
     rm ${infResultFile}
   fi
   cat ${infResultDir}/*  | awk '{print $1}'| sort | uniq | wc -l >> $infResultFile
   cat ${infResultDir}/*  | sort | uniq |  \
       awk '{sum[$2]+=1}END{for(key in sum){print key,sum[key]}}' | grep "^[0-9]\{1,3\} " >> $infResultFile
   cat $infResultFile | mail -s " inf category result ${day}" "yangbo@elex-tech.com"
}

function anaEstResult(){
  local day=$1
  local rootPath=/user/hadoop/user_category/lldaMahout
  local resultRoot=/data0/log/user_category_result/pr/total
  local estResultDir=${resultRoot}/${day}
  local estResultFile=${estResultDir}/analytics
  local tmpFile=${estResultDir}/tmp
  if test -e $estResultFile
    then
      rm $estResultFile
  fi
  cat ${estResultDir}/*  | awk '{print $1}'| wc -l >> $estResultFile
  if test -e $tmpFile
    then
      rm $tmpFile
  fi
  echo "cat ${estResultDir}/*  | awk '{sum[\$2]+=1}END{for(key in sum){print key,sum[key]}}'  >> $tmpFile"
  cat ${estResultDir}/*  | awk '{sum[$2]+=1}END{for(key in sum){print key,sum[key]}}'  >> $tmpFile
  cat ${tmpFile} | grep "^[0-9]\{1,3\} " >> $estResultFile
  cat ${estResultFile} | mail -s " est category analytics to${day}" "yangbo@elex-tech.com"
  hadoop fs -rm ${rootPath}/analysis/category/*
  hadoop fs -copyFromLocal  ${estResultDir}/0.0  ${rootPath}/analysis/category/userCategory
  hadoop fs -copyFromLocal  ${estResultFile} ${rootPath}/analysis/category/analytics
}

function anaCategoryDist(){
  local rootPath=/user/hadoop/user_category/lldaMahout
  local MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.CategoryAnalyzeTool
  local logFile=/data0/log/user_category/processLog/llda/analysis/categoryDist.log
  echo "hadoop jar $JAR $MAIN --input $1 --output $2 --category_result_dir ${rootPath}/analysis/category >> $logFile 2>&1"
  hadoop jar $JAR $MAIN --input $1 --output $2 --category_result_dir ${rootPath}/analysis/category >> $logFile 2>&1
}

function updateAnaCategoryDist(){
  local day=$1
  local nationDir=user_category/lldaMahout/nations
  local categoryAnaDir=user_category/lldaMahout/analysis/category
  local resultRoot=/data0/log/user_category_result/pr/total
  source $baseDir/bin/accumulateFuncs.sh
  updateNtByDay $day
  anaEstResult $day
  anaCategoryDist ${nationDir}/transTotal ${categoryAnaDir}/dist
  hadoop fs -cat ${categoryAnaDir}/dist/* > ${resultRoot}/${day}/categoryDist
  cat ${resultRoot}/${day}/categoryDist | mail -s " category dist result ${day}" "yangbo@elex-tech.com"
  local preDay=`date +%Y%m%d -d "$day -1 days"`
  compareCategoryDist $preDay $day

}

function compareCategoryDist(){
  local day=$1;local anoDay=$2
  local resultRoot=/data0/log/user_category_result/pr/total
  local file1=${resultRoot}/${day}/categoryDist; local file2=${resultRoot}/${anoDay}/categoryDist
  local resultFile=${resultRoot}/${day}/categoryDistComp
  python $baseDir/bin/compAnaResult.py $file1 $file2 > $resultFile
  yangBoMail=yangbo@elex-tech.com
  #chenShiHuaMail=chenshihua@elex-tech.com
  cat $resultFile | mail -s " category dist comp result ${anoDay}" "$yangBoMail $chenShiHuaMail"
}

function batchAnaCategoryDist(){
   local dayPattern=$1
   local resultRoot=/data0/log/user_category_result/pr/total
   files=`ls -l $resultRoot | grep $dayPattern | tr -s " " " " | cut -f9 -d" "`
   echo ${files[@]}
   source $baseDir/bin/accumulateFuncs.sh
   local nationBase=user_category/lldaMahout/nations
   for file in ${files[@]};do
    local day=$file
    anaEstResult $day
    transNtUid ${nationBase}/to${day}
    anaCategoryDist ${nationDir}/transTotal ${categoryAnaDir}/dist
    hadoop fs -cat ${categoryAnaDir}/dist/* > ${resultRoot}/${day}/categoryDist
    cat ${resultRoot}/${day}/categoryDist | mail -s " category dist result ${day}" "yangbo@elex-tech.com"
   done
   for file in ${files[@]};do
    local day=$file
    local preDay=`date +%Y%m%d -d "$day -1 days"`
    compareCategoryDist $preDay $day
   done
}


