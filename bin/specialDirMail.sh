#!/bin/bash
resultRoot=/data/log/user_category_result/pr
day=$1
resultDir=${resultRoot}/${day}
resultFile=${resultDir}/result
if test -e ${resultFile}
  rm ${resultFile}
categorys=(a b c d z)
cat ${resultDir}/* | wc -l >> ${resultDir}/result
for category in ${categorys[@]};do
   echo ${category}
   echo ${category} >> ${resultDir}/result
   cat ${resultDir}/* | grep ${category}$ | wc -l >> ${resultDir}/result
done
cat ${resultDir}/result | mail -s "category result ${day}" "yangbo@elex-tech.com"