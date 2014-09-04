package com.elex.bigdata.llda.mahout.data.hbase.nav;

import com.elex.bigdata.llda.mahout.data.hbase.RecordUnit;
import com.elex.bigdata.llda.mahout.data.hbase.ResultParser;
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
 * Date: 9/2/14
 * Time: 3:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class NavUrlTable extends NavTable{
  private  byte[] URL = Bytes.toBytes("url");
  //pId_1+nt_2+time_14(yyyyMMddHHmmss)+uid_
  private  int UID_INDEX = 17;

  @Override
  public Scan getScan(long startTime, long endTime) {
    List<String> columns=new ArrayList<String>();
    columns.add(Bytes.toString(URL));
    return getScan(startTime, endTime,columns);
  }

  @Override
  public ResultParser getResultParser() {
    return new NavUrlResultParser();
  }

  private  class NavUrlResultParser implements ResultParser {

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
        for (int i = 0; i < 2; i++)
          recordUnits.add(new RecordUnit(uid, url));
      }
      return recordUnits;
    }
  }
}
