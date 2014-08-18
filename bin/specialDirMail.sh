#!/bin/bash
resultRoot=/data0/log/user_category_result/pr
day=$1
resultDir=${resultRoot}/${day}
resultFile=${resultDir}/result
if test -e ${resultFile}
  then
  rm ${resultFile}
fi
categorys=(1 2 3 4 0)
cat ${resultDir}/* | awk '{print $1}' | sort | uniq | wc -l >> ${resultDir}/result
for category in ${categorys[@]};do
   echo ${category}
   echo ${category} >> ${resultDir}/result
   cat ${resultDir}/* | grep ${category}$ | awk '{print $1}' | sort | uniq | wc -l >> ${resultDir}/result
done
cat ${resultDir}/result | mail -s "category result ${day}" "yangbo@elex-tech.com"