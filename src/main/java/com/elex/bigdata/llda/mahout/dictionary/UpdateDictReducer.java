package com.elex.bigdata.llda.mahout.dictionary;

import com.elex.bigdata.hashing.BDMD5;
import com.elex.bigdata.hashing.HashingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/13/14
 * Time: 10:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class UpdateDictReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
  /*
     fields:
         dict: Dictionary
         word_count_threshold:
   */
  private Dictionary dict;
  private BDMD5 bdmd5;
  private int word_count_threshold;
  public void setup(Context context) throws IOException, InterruptedException {
    Configuration conf=context.getConfiguration();
    FileSystem fs=FileSystem.get(conf);
    word_count_threshold=Integer.parseInt(conf.get(UpdateDictDriver.COUNT_THRESHOLD));
    System.out.println("word count boundary is "+ word_count_threshold);
    String dictRoot=conf.get(UpdateDictDriver.DICT_ROOT);
    System.out.println("dict Root is "+dictRoot);
    try {
      dict=new Dictionary(dictRoot,fs,conf);
      bdmd5=BDMD5.getInstance();
    } catch (HashingException e) {
      e.printStackTrace();
    }
  }
  public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
     int wordCount=0;
     for(IntWritable countWritable:values){
        wordCount+=countWritable.get();
        if(wordCount>= word_count_threshold)
        {
          try {
            dict.update(bdmd5.toMD5(key.toString()));
          } catch (HashingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
          }
          return;
        }
     }
  }
  public void cleanup(Context context) throws IOException {
     dict.flushDict();
  }
}
