package com.elex.bigdata.llda.mahout.data.hbase.gm337;

import com.elex.bigdata.llda.mahout.data.hbase.RecordUnit;
import com.elex.bigdata.llda.mahout.data.hbase.ResultParser;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/2/14
 * Time: 3:10 PM
 * To change this template use File | Settings | File Templates.
 */
public class Gm337UrlTable extends Gm337Table{
  private  byte[] LAN = Bytes.toBytes("l"), GAME_TYPE = Bytes.toBytes("gt");
  //act_2+time_8+gmId_+\x01+uid_
  private  int GMID_INDEX=10;
  @Override
  public Scan getScan(long startTime, long endTime) {
    List<String> columns=new ArrayList<String>();
    columns.add(Bytes.toString(LAN));
    columns.add(Bytes.toString(GAME_TYPE));
    return getScan(startTime, endTime,columns);
  }

  @Override
  public ResultParser getResultParser() {
    return new Gm337UrlResultParser();
  }
  private  class Gm337UrlResultParser implements ResultParser{
    private String urlPre="www.337.com";
    private Map<String, String> gt2Url = new HashMap<String, String>();
    private int parseNum=0;
    public Gm337UrlResultParser(){
      gt2Url.put("web", "webgameplay");
      gt2Url.put("mini", "minigame/play");
    }
    @Override
    public List<RecordUnit> parse(Result result) {
      parseNum++;
      List<RecordUnit> recordUnits = new ArrayList<RecordUnit>();
      byte[] rk = result.getRow();
      int index;
      for (index = GMID_INDEX; index < rk.length; index++) {
        if (rk[index] == 1) {
          break;
        }
      }
      String game = Bytes.toString(Arrays.copyOfRange(rk, GMID_INDEX, index));
      String uid = Bytes.toString(Arrays.copyOfRange(rk, index + 1, rk.length));
      String lan=Bytes.toString(result.getValue(family, LAN));
      if(lan.length()>2)
        lan=lan.substring(0,2);
      String gt=Bytes.toString(result.getValue(family, GAME_TYPE));
      if(parseNum%50000==0)
        System.out.println("game type "+gt);
      String url = urlPre + File.separator + lan +
        File.separator + gt2Url.get(gt) + File.separator +
        game;
      for (int i = 0; i < 2; i++)
        recordUnits.add(new RecordUnit(uid, url));
      return recordUnits;
    }
  }
}
