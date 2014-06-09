package com.elex.bigdata.llda.mahout.data.accumulateurlcount;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/11/14
 * Time: 6:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class PutUrlCountRunnable implements Runnable {
  private Path filePath;
  private Map<String,Map<String,Integer>> uidUrlCountMap=new HashMap<String, Map<String, Integer>>();
  FileSystem fs;
  public PutUrlCountRunnable(FileSystem fs, Path filePath, Map<String, Map<String, Integer>> uidUrlCountMap) {
    this.filePath=filePath;
    this.uidUrlCountMap=uidUrlCountMap;
    this.fs=fs;
  }

  @Override
  public void run() {
    try {
      System.out.println(filePath.toString()+" uid size "+uidUrlCountMap.size());
      FSDataOutputStream outputStream=fs.create(filePath);
      BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(outputStream));
      for(Map.Entry<String,Map<String,Integer>> entry: uidUrlCountMap.entrySet()){
        String uid=entry.getKey();
        Map<String,Integer> urlCount=entry.getValue();

        for(Map.Entry<String,Integer> urlCountEntry: urlCount.entrySet()){
          StringBuilder builder=new StringBuilder();
          builder.append(uid+"\t");
          builder.append(urlCountEntry.getKey()+"\t"+urlCountEntry.getValue());
          writer.write(builder.toString());
          writer.write("\r\n");
        }
      }
      writer.flush();
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
}
