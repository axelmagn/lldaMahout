package com.elex.bigdata.llda.mahout.mapreduce.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/8/14
 * Time: 3:42 PM
 * To change this template use File | Settings | File Templates.
 */
public class WordExtractDriver {
  public static class WordExtractMapper extends Mapper<Object,Text,Text,IntWritable>{
    public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
      String[] tokens=value.toString().split("\t");
      if(tokens.length<3)
      {
        System.out.println("wrong line "+value.toString());
        return;
      }
      String shortWord=tokens[1];
      if(shortWord.startsWith("http://"))
        shortWord=shortWord.substring(7);
      else if(shortWord.startsWith("https://"))
        shortWord=shortWord.substring(8);
      if(shortWord.contains("/"))
        shortWord=shortWord.substring(0,shortWord.indexOf("/"));
      context.write(new Text(shortWord),new IntWritable(Integer.parseInt(tokens[2])));
    }
  }
  public static class WordExtractReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
    public void reduce(Text key,Iterable<IntWritable> values,Context context){

    }
  }
}
