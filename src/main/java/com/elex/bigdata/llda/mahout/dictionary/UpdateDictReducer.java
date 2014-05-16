package com.elex.bigdata.llda.mahout.dictionary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/13/14
 * Time: 10:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class UpdateDictReducer extends Reducer<Text,NullWritable,Text,IntWritable> {
  private Map<String,Integer> dict=new HashMap<String, Integer>();
  private int dictId;
  public void setup(Context context) throws IOException, InterruptedException {
    Configuration conf=context.getConfiguration();
    FileSystem fs=FileSystem.get(conf);
    Path dictPath=new Path(conf.get(UpdateDictDriver.DICT_PATH));
    if(!fs.exists(dictPath))
    {
      dictId=0;
      return;
    }
    SequenceFile.Reader dictReader=new SequenceFile.Reader(fs,dictPath,conf);
    Text url=new Text();
    IntWritable id=new IntWritable();
    while(dictReader.next(url,id)){
      dict.put(url.toString(),id.get());
      context.write(url,id);
    }
    dictId=dict.size();
  }
  public void reduce(Text key,Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
     if(!dict.containsKey(key.toString())){
       dict.put(key.toString(),dictId);
       context.write(key,new IntWritable(dictId));
       dictId++;
     }
  }
  public void cleanup(Context context) throws IOException {
     Configuration conf=context.getConfiguration();
     Path dictSizePath=new Path(conf.get(UpdateDictDriver.DICT_SIZE_PATH));
     SequenceFile.Writer dictSizeWriter=SequenceFile.createWriter(FileSystem.get(conf), conf, dictSizePath, IntWritable.class, NullWritable.class);
     dictSizeWriter.append(new IntWritable(dictId),NullWritable.get());
  }
}
