package com.elex.bigdata.llda.mahout.dictionary;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/13/14
 * Time: 10:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class UpdateDictMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
  public void map(LongWritable key,Text value ,Context context) throws IOException, InterruptedException {
     String[] uidUrlCount=value.toString().split("\t");
     String url=uidUrlCount[1];
     context.write(new Text(url), NullWritable.get());
  }
}
