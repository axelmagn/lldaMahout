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
  private Path dictPath,dictSizePath,tmpDictPath;
  public void setup(Context context) throws IOException, InterruptedException {
    Configuration conf=context.getConfiguration();
    FileSystem fs=FileSystem.get(conf);
    dictPath=new Path(conf.get(UpdateDictDriver.DICT_PATH));
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
      //context.write(url,id);
    }
    dictId=dict.size();
  }
  public void reduce(Text key,Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
     if(!dict.containsKey(key.toString())){
       dict.put(key.toString(),dictId);
       //context.write(key,new IntWritable(dictId));
       dictId++;
     }
  }
  public void cleanup(Context context) throws IOException {
     Configuration conf=context.getConfiguration();
     FileSystem fs=FileSystem.get(conf);
     dictSizePath=new Path(conf.get(UpdateDictDriver.DICT_SIZE_PATH));
     if(fs.exists(dictSizePath))
       fs.delete(dictSizePath);
     SequenceFile.Writer dictSizeWriter=SequenceFile.createWriter(fs, conf, dictSizePath, IntWritable.class, NullWritable.class);
     dictSizeWriter.append(new IntWritable(dictId),NullWritable.get());
     dictSizeWriter.hflush();
     dictSizeWriter.close();
     tmpDictPath=new Path(conf.get(UpdateDictDriver.TMP_DICT_PATH));
     if(fs.exists(tmpDictPath))
       fs.delete(tmpDictPath);
     SequenceFile.Writer tmpDictWriter=SequenceFile.createWriter(FileSystem.get(conf), conf, tmpDictPath, Text.class, IntWritable.class);
     for(Map.Entry<String,Integer> entry: dict.entrySet()){
        tmpDictWriter.append(new Text(entry.getKey()),new IntWritable(entry.getValue()));
     }
     tmpDictWriter.hflush();
     tmpDictWriter.close();
     if(fs.exists(dictPath))
       fs.delete(dictPath);
     fs.rename(tmpDictPath,dictPath);
  }
}
