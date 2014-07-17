#!/bin/bash
day=`date +%Y%m%d -d "-1 days"`
baseDir=`dirname $0`/..
sh ${baseDir}/bin/specialDirMail.sh $day
