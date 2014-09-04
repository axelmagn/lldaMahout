package com.elex.bigdata.llda.mahout.data.hbase.custom;

import com.elex.bigdata.llda.mahout.data.hbase.SuperTable;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/28/14
 * Time: 11:18 AM
 * To change this template use File | Settings | File Templates.
 */
public abstract class CustomTable implements SuperTable {
  protected byte[] family = Bytes.toBytes("ua");
  protected Scan getScan(long startTime,long endTime,String column){
    Scan scan = new Scan();
    byte[] startRk = Bytes.add(Bytes.toBytesBinary("\\x01"), Bytes.toBytes(startTime));
    byte[] endRk = Bytes.add(Bytes.toBytesBinary("\\x01"), Bytes.toBytes(endTime));
    System.out.println("start: " + Bytes.toStringBinary(startRk) + ", end: " + Bytes.toStringBinary(endRk));
    scan.setStartRow(startRk);
    scan.setStopRow(endRk);
    scan.addColumn(family, Bytes.toBytes(column));
    int cacheSize = 10000;
    scan.setBatch(10);
    scan.setCaching(cacheSize);
    return scan;
  }




}
