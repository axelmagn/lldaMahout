package com.elex.bigdata.llda.mahout.data.accumulateurlcount.tables;

import com.elex.bigdata.llda.mahout.data.accumulateurlcount.RecordUnit;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/28/14
 * Time: 11:18 AM
 * To change this template use File | Settings | File Templates.
 */
public class CustomTable implements SuperTable{
  private static byte[] family = Bytes.toBytes("ua");
  private static byte[] URL = Bytes.toBytes("url");
  private static int UID_INDEX = 9;

  public Scan getScan(long startTime, long endTime) {
    Scan scan = new Scan();
    byte[] startRk = Bytes.add(Bytes.toBytesBinary("\\x01"), Bytes.toBytes(startTime));
    byte[] endRk = Bytes.add(Bytes.toBytesBinary("\\x01"), Bytes.toBytes(endTime));
    System.out.println("start: " + Bytes.toStringBinary(startRk) + ", end: " + Bytes.toStringBinary(endRk));
    scan.setStartRow(startRk);
    scan.setStopRow(endRk);
    scan.addColumn(family, URL);
    int cacheSize = 10000;
    scan.setBatch(10);
    scan.setCaching(cacheSize);
    return scan;
  }

  @Override
  public ResultParser getResultParser() {
    return new CustomResultParser();
  }

  private static class CustomResultParser implements ResultParser {

    @Override
    public List<RecordUnit> parse(Result result) {
      List<RecordUnit> recordUnits = new ArrayList<RecordUnit>();
      for (KeyValue kv : result.raw()) {
        byte[] rk = kv.getRow();
        String uid = Bytes.toString(Arrays.copyOfRange(rk, UID_INDEX, rk.length));
        String url = Bytes.toString(kv.getValue());
        if (url.startsWith("http://"))
          url = url.substring(7);
        if (url.startsWith("https://"))
          url = url.substring(8);
        if (url.endsWith("/"))
          url = url.substring(0, url.length() - 1);
        recordUnits.add(new RecordUnit(uid, url, 1));
      }
      return recordUnits;
    }
  }
}
