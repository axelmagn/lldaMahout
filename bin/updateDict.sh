#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
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