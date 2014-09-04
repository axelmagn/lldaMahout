package com.elex.bigdata.llda.mahout.data.accumulate;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/2/14
 * Time: 5:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class AccumulateNtReducer extends Reducer<Text,Text,Text,NullWritable> {
  public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
    context.write(key, NullWritable.get());
  }

}
