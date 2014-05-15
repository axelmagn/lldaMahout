package com.elex.bigdata.llda.mahout.data.generatedocs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 10:36 AM
 * To change this template use File | Settings | File Templates.
 */
public class GenerateLDocMapper extends Mapper<LongWritable,Text,Text,Text> {
  public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
    /*
        extract uid and url-count from value
        while loopCount < count
           context.write(uid,url)
     */
    String[] uidUrlCount=value.toString().split("\t");
    int count=Integer.parseInt(uidUrlCount[2]);
    for(int i=0;i<count;i++){
      context.write(new Text(uidUrlCount[0]),new Text(uidUrlCount[1]));
    }
  }
}
