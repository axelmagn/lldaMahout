package com.elex.bigdata.llda.mahout.data.hbase.ad;

import com.elex.bigdata.llda.mahout.data.hbase.RecordUnit;
import com.elex.bigdata.llda.mahout.data.hbase.ResultParser;
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
 * Time: 2:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class AdNtTable extends AdTable {
  private static byte[] CATEGORY = Bytes.toBytes("c");
  //pId_1+nt_2+time_8+uid_
  private int UID_INDEX = 11,NT_INDEX_START=1,NT_INDEX_END=3;
  @Override
  public Scan getScan(long startTime, long endTime) {
    List<String> columns=new ArrayList<String>();
    columns.add(Bytes.toString(CATEGORY));
    return getScan(startTime, endTime,columns);
  }
  @Override
  public ResultParser getResultParser(){
    return new AdNtResultParser();
  }
  private class AdNtResultParser implements ResultParser{

    @Override
    public List<RecordUnit> parse(Result result) {
      byte[] rk=result.getRow();
      String uid=Bytes.toString(Arrays.copyOfRange(rk,UID_INDEX,rk.length));
      String nt=Bytes.toString(Arrays.copyOfRange(rk,NT_INDEX_START,NT_INDEX_END));
      List<RecordUnit> recordUnits=new ArrayList<RecordUnit>();
      recordUnits.add(new RecordUnit(uid,nt));
      return recordUnits;
    }
  }
}
