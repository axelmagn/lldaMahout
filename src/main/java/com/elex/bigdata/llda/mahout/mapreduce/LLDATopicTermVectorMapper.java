package com.elex.bigdata.llda.mahout.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/27/14
 * Time: 3:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class LLDATopicTermVectorMapper extends Mapper<IntWritable,VectorWritable,IntWritable,VectorWritable> {
  public void map(IntWritable key,VectorWritable value,Context context) throws IOException, InterruptedException {
    context.write(key,value);
  }
}
