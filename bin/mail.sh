#!/bin/bash
resultRoot=/data/log/user_category_result/pr
day=`date +%Y%m%d -d "-1 days"`
resultDir=${resultRoot}/${day}
categorys=(a b c d z)
for category in categorys;do
   echo ${category}
   echo ${category} >> ${resultDir}/result
   cat ${resultDir}/* | grep ${category}$ | wc -l >> ${resultDir}/result
done
cat ${resultDir}/result | mail -s "category result ${day}" "yangbo@elex-tech.com"

