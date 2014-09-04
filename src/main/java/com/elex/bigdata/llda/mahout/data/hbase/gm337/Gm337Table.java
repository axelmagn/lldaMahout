package com.elex.bigdata.llda.mahout.data.hbase.gm337;

import com.elex.bigdata.llda.mahout.data.hbase.RecordUnit;
import com.elex.bigdata.llda.mahout.data.hbase.ResultParser;
import com.elex.bigdata.llda.mahout.data.hbase.SuperTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/28/14
 * Time: 2:10 PM
 * To change this template use File | Settings | File Templates.
 */
public abstract class Gm337Table implements SuperTable {
  protected byte[] family = Bytes.toBytes("ua");


  protected Scan getScan(long startTime,long endTime,List<String> columns){
    Scan scan = new Scan();
    for(String column: columns)
      scan.addColumn(family,Bytes.toBytes(column));
    scan.setStartRow(Bytes.add(Bytes.toBytes("pl"), Bytes.toBytes(startTime)));
    scan.setStopRow(Bytes.add(Bytes.toBytes("pl"), Bytes.toBytes(endTime)));
    return scan;
  }



}
