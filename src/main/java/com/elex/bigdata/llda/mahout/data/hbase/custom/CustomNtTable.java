package com.elex.bigdata.llda.mahout.data.hbase.custom;

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
 * Time: 2:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class CustomNtTable extends CustomTable{
  private  byte[] NT = Bytes.toBytes("nt");
  private  int UID_INDEX = 9;
  @Override
  public Scan getScan(long startTime, long endTime) {
    return getScan(startTime, endTime,Bytes.toString(NT));
  }

  @Override
  public ResultParser getResultParser() {
    return new CustomNtResultParser();
  }

  private class CustomNtResultParser implements ResultParser{

    @Override
    public List<RecordUnit> parse(Result result) {
      byte[] rk=result.getRow();
      String uid=Bytes.toString(Arrays.copyOfRange(rk,UID_INDEX,rk.length));
      String nt=Bytes.toString(result.getValue(family,NT));
      List<RecordUnit> recordUnits=new ArrayList<RecordUnit>();
      recordUnits.add(new RecordUnit(uid,nt));
      return recordUnits;
    }
  }

}
