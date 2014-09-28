#!/bin/bash
baseDir=`dirname $0`/..
JAR=$baseDir/target/lldaMahout-1.0-SNAPSHOT-jar-with-dependencies.jar
source $baseDir/bin/accumulateFuncs.sh
if [ $# = 1 ] ; then
    day=$1
else
    day=`date -d "1 days ago" +%Y%m%d`
fi
countNt ${day}

python $baseDir/bin/count_user_nation.py ${day}




