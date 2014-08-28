package com.elex.bigdata.llda.mahout.data.accumulateurlcount.tables;

import org.apache.hadoop.hbase.client.Scan;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/28/14
 * Time: 11:39 AM
 * To change this template use File | Settings | File Templates.
 */
public interface SuperTable {
  Scan getScan(long startTime,long endTime);
  ResultParser getResultParser();
}
