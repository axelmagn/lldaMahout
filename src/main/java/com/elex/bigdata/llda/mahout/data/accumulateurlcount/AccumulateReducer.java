package com.elex.bigdata.llda.mahout.data.accumulateurlcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/28/14
 * Time: 2:34 PM
 * To change this template use File | Settings | File Templates.
 */
public class AccumulateReducer extends Reducer<Text,IntWritable,Text,Text> {
  public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
    int sum=0;
    for(IntWritable count: values){
      sum+=count.get();
    }
    context.write(new Text(key),new Text(String.valueOf(sum)));
  }
}
