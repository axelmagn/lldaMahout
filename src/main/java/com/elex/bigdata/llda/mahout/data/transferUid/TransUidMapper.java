package com.elex.bigdata.llda.mahout.data.transferUid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/3/14
 * Time: 3:50 PM
 * To change this template use File | Settings | File Templates.
 */
public  class TransUidMapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
  private HTable uid2CookieMap;
  private byte[] family = Bytes.toBytes("cu"), idColumn = Bytes.toBytes("uid");
  protected Map<String, String> uid2CookieId = new HashMap<String, String>();
  protected int uidNum = 0;
  public void setup(Context context) throws IOException {
    Configuration conf=context.getConfiguration();
    uid2CookieMap=new HTable(conf,"cookie_uid_map");
  }
  protected void getCookieIds(Set<String> uids) throws IOException {
    List<Get> gets = new ArrayList<Get>();
    for (String uid : uids) {
      Get get = new Get(Bytes.toBytes("u_" + uid));
      get.addColumn(family, idColumn);
      gets.add(get);
    }
    for (Result result : uid2CookieMap.get(gets)) {
      for (KeyValue kv : result.raw()) {
        uid2CookieId.put(Bytes.toString(kv.getKey()).substring(2), Bytes.toString(kv.getValue()));
      }
    }
  }

  public void cleanup(Context context) throws IOException {
    uid2CookieMap.close();
  }
}
