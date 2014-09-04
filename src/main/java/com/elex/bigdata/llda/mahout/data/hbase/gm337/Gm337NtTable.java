package com.elex.bigdata.llda.mahout.data.hbase.gm337;

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
 * Time: 3:10 PM
 * To change this template use File | Settings | File Templates.
 */
public class Gm337NtTable extends Gm337Table {
  private  byte[] NT = Bytes.toBytes("nt");
  //act_2+time_8+gmId_+\x01+uid_
  private  int GMID_INDEX=10;
  @Override
  public Scan getScan(long startTime, long endTime) {
    List<String> columns=new ArrayList<String>();
    columns.add(Bytes.toString(NT));
    return getScan(startTime, endTime,columns);
  }

  @Override
  public ResultParser getResultParser() {
    return new Gm337NtResultParser();
  }

  private class Gm337NtResultParser implements ResultParser{

    @Override
    public List<RecordUnit> parse(Result result) {
      byte[] rk = result.getRow();
      int index;
      for (index = GMID_INDEX; index < rk.length; index++) {
        if (rk[index] == 1) {
          break;
        }
      }
      String uid = Bytes.toString(Arrays.copyOfRange(rk, index + 1, rk.length));
      String nt=Bytes.toString(result.getValue(family,NT));
      List<RecordUnit> recordUnits=new ArrayList<RecordUnit>();
      recordUnits.add(new RecordUnit(uid,nt));
      return recordUnits;
    }
  }
}
