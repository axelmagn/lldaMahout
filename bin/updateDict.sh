#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
MAIN=com.elex.bigdata.llda.mahout.dictionary.UpdateDictDriver
rootPath=/user/hadoop/user_category/lldaMahout
logFile=/data/log/user_category/processLog/llda/updateDict.log
echo `date` >> $logFile
if [ $# -ge 2 ];then
  echo "hadoop jar $JAR $MAIN --input $1 --count_threshold $2 --dict_root ${rootPath}/dictionary >> $logFile 2>&1"
  hadoop jar $JAR $MAIN --input $1 --count_threshold $2 --dict_root ${rootPath}/dictionary  >> $logFile 2>&1
else
  echo "hadoop jar $JAR $MAIN --input $1  --dict_root ${rootPath}/dictionary >> $logFile 2>&1"
  hadoop jar $JAR $MAIN --input $1  --dict_root ${rootPath}/dictionary  >> $logFile 2>&1
fi