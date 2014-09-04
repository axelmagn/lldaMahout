#!/bin/bash
baseDir=`dirname $0`/..
startTime=`date +%Y%m%d%H%M%S -d "-1 hours -20 mins"`
endTime=`date +%Y%m%d%H%M%S -d "-20 mins"`
source $baseDir/bin/infEstFuncs.sh
infOrigData $startTime $endTime

