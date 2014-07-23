#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar

rootPath=/user/hadoop/user_category/lldaMahout
logFile=/data/log/user_category/processLog/llda/analysis.log
if [[ $# < 3 ]]
then
  echo " args < 3"
  exit 1
fi
type=$1
if [[ $type = userTrained ]]
  then
     MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.UserTrainedDriver
     echo "hadoop jar $JAR $MAIN --input ${rootPath}/$2 --output ${rootPath}/$3 >> $logFile 2>&1"
     hadoop jar $JAR $MAIN --input ${rootPath}/$2 --output ${rootPath}/$3 >> $logFile 2>&1
  elif [[ $type = user ]]
  then
     MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.WordCountByUserDriver
     echo "hadoop jar $JAR $MAIN --input $2 --output $3 >> $logFile 2>&1"
     hadoop jar $JAR $MAIN --input $2 --output $3 >> $logFile 2>&1
  elif [[ $type = uniqWord ]]
    then
     MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.WordUniqDriver
     echo "hadoop jar $JAR $MAIN $2 ${rootPath}/analysis/tmp $3 >> $logFile 2>&1"
     hadoop jar $JAR $MAIN $2 ${rootPath}/analysis/tmp $3 >> $logFile 2>&1
  elif [[ $type = wordCount ]]
     then
     MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.WordCountDriver
     echo "hadoop jar $JAR $MAIN --input $2 --output $3 >> $logFile 2>&1"
     hadoop jar $JAR $MAIN --input $2 --output $3 >> $logFile 2>&1
  elif [[ $type = wordExtract ]]
     then
     MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.WordExtractDriver
     echo "hadoop jar $JAR $MAIN --input $2 --output $3 >> $logFile 2>&1"
     hadoop jar $JAR $MAIN --input $2 --output $3 >> $logFile 2>&1
  elif [[ $type = dictWord ]]
     then
     MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.GetDictWordDriver
     echo "hadoop jar $JAR $MAIN --input $2 --output $3 >> $logFile 2>&1"
     hadoop jar $JAR $MAIN --input $2 --output $3 >> $logFile 2>&1
  elif [[ $type = wordClean ]]
     then
     MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.WordCleanDriver
     echo "hadoop jar $JAR $MAIN --input $2 --output $3 >> $logFile 2>&1"
     hadoop jar $JAR $MAIN --input $2 --output $3 >> $logFile 2>&1
  else
     MAIN=com.elex.bigdata.llda.mahout.mapreduce.analysis.WordLenDriver
     echo "hadoop jar $JAR $MAIN $2 $3 >> $logFile 2>&1"
     hadoop jar $JAR $MAIN $2 $3 >> $logFile 2>&1
fi

