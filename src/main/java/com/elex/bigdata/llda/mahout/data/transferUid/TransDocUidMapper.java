package com.elex.bigdata.llda.mahout.data.transferUid;

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

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/14/14
 * Time: 5:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class TransDocUidMapper extends TransUidMapper<Text, MultiLabelVectorWritable, Text, MultiLabelVectorWritable> {
  private Map<String, String> uid2CookieId = new HashMap<String, String>();
  private Map<String, MultiLabelVectorWritable> uid2Doc = new HashMap<String, MultiLabelVectorWritable>();


  public void map(Text key, MultiLabelVectorWritable value, Context context) throws IOException, InterruptedException {
    uid2Doc.put(key.toString(), value);
    uidNum++;
    if (uidNum % 10000 == 1) {
      getCookieIds(uid2Doc.keySet());
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


  public void cleanup(Context context) throws IOException {
    getCookieIds(uid2CookieId.keySet());
    for (Map.Entry<String, MultiLabelVectorWritable> entry : uid2Doc.entrySet()) {
      String cookieId = uid2CookieId.get(entry.getKey());
      if (cookieId == null)
        cookieId = entry.getKey();
      try {
        context.write(new Text(cookieId), entry.getValue());
      } catch (InterruptedException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
    super.cleanup(context);
  }
}
