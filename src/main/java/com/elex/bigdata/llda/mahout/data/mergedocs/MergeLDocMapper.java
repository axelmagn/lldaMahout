package com.elex.bigdata.llda.mahout.data.mergedocs;

import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver;
import com.elex.bigdata.llda.mahout.dictionary.UpdateDictDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.MultiLabelVectorWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 10:27 AM
 * To change this template use File | Settings | File Templates.
 */
public class MergeLDocMapper extends Mapper<Text, MultiLabelVectorWritable, Text, MultiLabelVectorWritable> {
  private Map<String,String> uid2CookieId = new HashMap<String,String>();
  private boolean filtering, useCookieId;

  public void setup(Context context) throws IOException {
    /*
       extract uid from uidFile
     */
    filtering = false;
    useCookieId = false;
    Configuration conf = context.getConfiguration();
    initFiltering(conf);
  }

  private void initFiltering(Configuration conf) throws IOException {
    String uid_file = conf.get(GenerateLDocDriver.UID_PATH);
    if (uid_file == null) {
      System.out.println("uid file is " + uid_file);
      return;
    }
    Path uidPath = new Path(uid_file);
    SequenceFile.Reader uidReader = new SequenceFile.Reader(FileSystem.get(conf), uidPath, conf);
    Text uid = new Text();
    NullWritable nullWritable = NullWritable.get();
    if (conf.get(MergeLDocDriver.USE_COOKIEID) != null)
      useCookieId = true;
    if (useCookieId) {
      HTable table=new HTable(HBaseConfiguration.create(),"cookie_uid_map");
      List<Get> gets=new ArrayList<Get>();
      byte[] family=Bytes.toBytes("cu"),idColumn=Bytes.toBytes("uid");
      int uidNum=0;
      while (uidReader.next(uid, nullWritable)) {
        //uids.add(uid.toString());
        uidNum++;
        uid2CookieId.put(uid.toString(),uid.toString());
        Get get=new Get(Bytes.toBytes("u_"+ uid.toString()));
        get.addColumn(family,idColumn);
        gets.add(get);
        if(uidNum%10000==1){
           getMoreUids(table,gets);
           gets.clear();
        }
      }
      getMoreUids(table,gets);

    } else {
      while (uidReader.next(uid, nullWritable)) {
        uid2CookieId.put(uid.toString(),uid.toString());
      }
    }
    System.out.println("uids is " + uid2CookieId.size());
    uidReader.close();
    filtering = true;
    System.out.println("filtering is " + filtering);
  }

  private void getMoreUids(HTable table,List<Get> cookieIdGets) throws IOException {
    List<Get> uidGets=new ArrayList<Get>();
    byte[] family=Bytes.toBytes("cu"),idColumn=Bytes.toBytes("uid");
    for(Result result: table.get(cookieIdGets)){
      byte[] rk=result.getRow();
      String uid=Bytes.toString(rk).substring(2);
      for(KeyValue kv :result.raw()){
        String cookieId=Bytes.toString(kv.getValue());
        uid2CookieId.put(uid,cookieId);
        Get uidGet =new Get(Bytes.toBytes("c_"+cookieId));
        uidGet.setMaxVersions(50);
        uidGet.addColumn(family,idColumn);
        uidGets.add(uidGet);
      }
    }
    for(Result result :table.get(uidGets)){
      String cookieId=Bytes.toString(result.getRow()).substring(2);
      for(KeyValue kv : result.raw()){
        uid2CookieId.put(Bytes.toString(kv.getValue()),cookieId);
      }
    }
  }

  public void map(Text key, MultiLabelVectorWritable value, Context context) throws IOException, InterruptedException {
     /*
        create a labeledDocument with size of dictSize according to value
     */
    if (!filtering ){
      context.write(key, value);
      return;
    }
    if(uid2CookieId.containsKey(key.toString())){
      context.write(new Text(uid2CookieId.get(key.toString())), value);
    }
  }
}
