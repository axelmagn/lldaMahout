package com.elex.bigdata.llda.mahout.dictionary;

import com.elex.bigdata.llda.mahout.data.inputformat.CombineTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 11:45 AM
 * To change this template use File | Settings | File Templates.
 */
public class UpdateDictDriver extends AbstractJob{
  public static final String DICT_ROOT ="dict_root";
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOption(DICT_ROOT,"dict","dictionary root Path");
    if(parseArguments(args)==null)
      return -1;
    Path textInputPath=getInputPath();
    String dictRoot=getOption(DICT_ROOT);
    Configuration conf=new Configuration();
    Job updateDictJob=prepareJob(conf,textInputPath,new Path(dictRoot));
    updateDictJob.submit();
    updateDictJob.waitForCompletion(true);
    return 0;
  }
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new UpdateDictDriver(),args);
  }

  public static Job prepareJob(Configuration conf,Path inputPath,Path dictRootPath) throws IOException {
    conf.setLong("mapred.max.split.size", 22485760); // 10m
    conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 22485760);
    conf.set(DICT_ROOT,dictRootPath.toString());
    Path dictOutputPath=new Path(dictRootPath,"updateDictOut");
    FileSystem fs=FileSystem.get(conf);
    if(fs.exists(dictOutputPath))
      fs.delete(dictOutputPath);

    Job updateDictJob=new Job(conf);
    updateDictJob.setInputFormatClass(CombineTextInputFormat.class);
    updateDictJob.setMapperClass(UpdateDictMapper.class);
    updateDictJob.setReducerClass(UpdateDictReducer.class);
    FileInputFormat.addInputPath(updateDictJob, inputPath);
    SequenceFileOutputFormat.setOutputPath(updateDictJob, dictOutputPath);
    updateDictJob.setOutputFormatClass(SequenceFileOutputFormat.class);
    updateDictJob.setMapOutputKeyClass(Text.class);
    updateDictJob.setMapOutputValueClass(IntWritable.class);
    updateDictJob.setJarByClass(UpdateDictDriver.class);
    updateDictJob.setJobName("update Dict");
    return updateDictJob;
  }
}
