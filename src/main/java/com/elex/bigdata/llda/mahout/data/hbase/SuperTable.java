package com.elex.bigdata.llda.mahout.data.hbase;

import org.apache.hadoop.hbase.client.Scan;

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/28/14
 * Time: 11:39 AM
 * To change this template use File | Settings | File Templates.
 */
public interface SuperTable {
  /*
     interface for multi tables
     map from tableName to tableClass saved in 'table_type.xml'
   */
  Scan getScan(long startTime,long endTime);
  ResultParser getResultParser();
}
