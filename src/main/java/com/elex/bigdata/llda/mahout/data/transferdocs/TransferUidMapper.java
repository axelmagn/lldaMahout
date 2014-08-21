package com.elex.bigdata.llda.mahout.data.transferdocs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.MultiLabelVectorWritable;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/14/14
 * Time: 5:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class TransferUidMapper extends Mapper<Text, MultiLabelVectorWritable, Text, MultiLabelVectorWritable> {
  private Map<String, String> uid2CookieId = new HashMap<String, String>();
  private Map<String, MultiLabelVectorWritable> uid2Doc = new HashMap<String, MultiLabelVectorWritable>();
  private int uidNum = 0;
  private HTable uid2CookieMap;
  private byte[] family = Bytes.toBytes("cu"), idColumn = Bytes.toBytes("uid");

  public void setup(Context context) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    uid2CookieMap = new HTable(conf, "cookie_uid_map");
  }

  public void map(Text key, MultiLabelVectorWritable value, Context context) throws IOException, InterruptedException {
    uid2Doc.put(key.toString(), value);
    uidNum++;
    if (uidNum % 10000 == 1) {
      getCookieIds();
      int sampleRatio=1;
      for (Map.Entry<String, MultiLabelVectorWritable> entry : uid2Doc.entrySet()) {
        sampleRatio++;
        String cookieId = uid2CookieId.get(entry.getKey());
        if (cookieId == null)
          cookieId = entry.getKey();
        context.write(new Text(cookieId), entry.getValue());
        if(sampleRatio%1000==0){
           System.out.println("cookieId:"+cookieId);
        }
      }
      uid2CookieId=new HashMap<String, String>();
      uid2Doc=new HashMap<String, MultiLabelVectorWritable>();

    }
  }

  private void getCookieIds() throws IOException {
    List<Get> gets = new ArrayList<Get>();
    for (String uid : uid2Doc.keySet()) {
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

  public void cleanup(Context context) throws IOException, InterruptedException {
    getCookieIds();
    for (Map.Entry<String, MultiLabelVectorWritable> entry : uid2Doc.entrySet()) {
      String cookieId = uid2CookieId.get(entry.getKey());
      if (cookieId == null)
        cookieId = entry.getKey();
      context.write(new Text(cookieId), entry.getValue());
    }
    uid2CookieMap.close();
  }
}
