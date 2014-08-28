package com.elex.bigdata.llda.mahout.data.accumulateurlcount.tables;

import com.elex.bigdata.llda.mahout.data.accumulateurlcount.RecordUnit;
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
public class Gm337Table implements SuperTable {
  private static byte[] family = Bytes.toBytes("ua");
  private static byte[] LAN = Bytes.toBytes("l"), GAME_TYPE = Bytes.toBytes("gt");
  @Override
  public Scan getScan(long startTime, long endTime) {
    Scan scan = new Scan();
    scan.addColumn(family, LAN);
    scan.addColumn(family, LAN);
    scan.setStartRow(Bytes.add(Bytes.toBytes("pl"), Bytes.toBytes(startTime)));
    scan.setStopRow(Bytes.add(Bytes.toBytes("pl"), Bytes.toBytes(endTime)));
    return scan;
  }

  @Override
  public ResultParser getResultParser() {
    return new Gm337ResultParser();
  }

  private static class Gm337ResultParser implements ResultParser{
    private String urlPre="www.337.com";
    private Map<String, String> gt2Url = new HashMap<String, String>();
    public Gm337ResultParser(){
      gt2Url.put("web", "webgameplay");
      gt2Url.put("mini", "minigame/play");
    }
    @Override
    public List<RecordUnit> parse(Result result) {
      List<RecordUnit> recordUnits = new ArrayList<RecordUnit>();
      byte[] rk = result.getRow();
      int index;
      for (index = 11; index < rk.length; index++) {
        if (rk[index] == 1) {
          break;
        }
      }
      String game = Bytes.toString(Arrays.copyOfRange(rk, 11, index));
      String uid = Bytes.toString(Arrays.copyOfRange(rk, index + 1, rk.length));
      String lan=Bytes.toString(result.getValue(family, LAN));
      if(lan.length()>2)
        lan=lan.substring(0,2);
      String url = urlPre + File.separator + lan +
        File.separator + gt2Url.get(Bytes.toString(result.getValue(family, GAME_TYPE))) + File.separator +
        game;
      recordUnits.add(new RecordUnit(uid,url,2));
      return recordUnits;
    }
  }
}
