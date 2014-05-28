#!/bin/bash
baseDir=`dirname $0`/..
preDay=`date +%Y%m%d -d "-1 days"`
day=`date +%Y%m%d `
sh ${baseDir}/bin/getEstDocs.sh url_count/*/${day}* to${preDay} ${day}
sh ${baseDir}/bin/estDocs.sh to${day}