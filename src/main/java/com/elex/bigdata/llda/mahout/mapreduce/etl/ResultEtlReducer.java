package com.elex.bigdata.llda.mahout.mapreduce.etl;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/3/14
 * Time: 1:54 PM
 * To change this template use File | Settings | File Templates.
 */
public class ResultEtlReducer extends Reducer<Text,Text,Text,Text> {
  public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
    context.write(key,values.iterator().next());
  }
}
