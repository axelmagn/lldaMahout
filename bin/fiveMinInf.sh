#!/bin/bash
baseDir=`dirname $0`/..
preDay=`date +%Y%m%d -d "-1 days"`
day=`date +%Y%m%d `
sh $baseDir/bin/getInfDocs.sh url_count/*/${day}* to${preDay} ${day}
sh $baseDir/bin/infDocs.sh ${day}
sh $baseDir/bin/etl.sh inf result