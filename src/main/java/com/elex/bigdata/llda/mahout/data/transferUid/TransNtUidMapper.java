package com.elex.bigdata.llda.mahout.data.transferUid;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.MultiLabelVectorWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/3/14
 * Time: 3:46 PM
 * To change this template use File | Settings | File Templates.
 */
public class TransNtUidMapper extends TransUidMapper<Object, Text, Text, Text> {
  private Map<String, String> uid2Nation = new HashMap<String, String>();

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String[] tokens = value.toString().split("\t");
    uid2Nation.put(tokens[0], tokens[1]);
    uidNum++;
    if (uidNum % 10000 == 1) {
      getCookieIds(uid2Nation.keySet());
      int sampleRatio = 1;
      for (Map.Entry<String, String> entry : uid2Nation.entrySet()) {
        sampleRatio++;
        String cookieId = uid2CookieId.get(entry.getKey());
        if (cookieId == null)
          cookieId = entry.getKey();
        context.write(new Text(cookieId), new Text(entry.getValue()));
        if (sampleRatio % 10000 == 0) {
          System.out.println("cookieId:" + cookieId);
        }
      }
      uid2CookieId = new HashMap<String, String>();
      uid2Nation = new HashMap<String, String>();
    }
  }

  public void cleanup(Context context) throws IOException {
    getCookieIds(uid2Nation.keySet());
    for (Map.Entry<String, String> entry : uid2Nation.entrySet()) {
      String cookieId = uid2CookieId.get(entry.getKey());
      if (cookieId == null)
        cookieId = entry.getKey();
      try {
        context.write(new Text(cookieId), new Text(entry.getValue()));
      } catch (InterruptedException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
    super.cleanup(context);
  }

}
