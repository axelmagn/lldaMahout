package com.elex.bigdata.llda.mahout.dictionary;

import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver;
import com.elex.bigdata.llda.mahout.data.inputformat.CombineTextInputFormat;
import com.elex.bigdata.llda.mahout.util.FileSystemUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import java.io.File;
import java.io.IOException;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 11:45 AM
 * To change this template use File | Settings | File Templates.
 */
public class UpdateDictDriver extends AbstractJob{
  public static final String DICT_ROOT ="dict_root";
  public static final String COUNT_THRESHOLD="count_threshold";
  public static final String COUNT_UPPER_THRESHOLD="count_upper_threshold";
  public static final int DEFAULT_COUNT_THRESHOLD=5;
  public static final int DEFUAL_COUNT_UPPER_THRESHOLD=100000;
  public static final int MD5_START_INDEX=6,MD5_END_INDEX=18;
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOption(DICT_ROOT,"dict","dictionary root Path");
    addOption(COUNT_THRESHOLD,"count_threshold","specify the word count threshold to add to dictionary",false);
    addOption(COUNT_UPPER_THRESHOLD,"count_upper_threshold","specify the word count uppper threshold to add to dictionary",false);
    addOption(GenerateLDocDriver.RESOURCE_ROOT,"rDir","specify the resources Dir",true);
    if(parseArguments(args)==null)
      return -1;
    Path textInputPath=getInputPath();
    String dictRoot=getOption(DICT_ROOT);
    Configuration conf=new Configuration();
    conf.set(GenerateLDocDriver.RESOURCE_ROOT,getOption(GenerateLDocDriver.RESOURCE_ROOT));
    conf.set(COUNT_THRESHOLD,getOption(COUNT_THRESHOLD,String.valueOf(DEFAULT_COUNT_THRESHOLD)));
    conf.set(COUNT_UPPER_THRESHOLD,getOption(COUNT_UPPER_THRESHOLD,String.valueOf(DEFUAL_COUNT_UPPER_THRESHOLD)));
    Job updateDictJob=prepareJob(conf,textInputPath,new Path(dictRoot));
    updateDictJob.submit();
    updateDictJob.waitForCompletion(true);
    return 0;
  }
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new UpdateDictDriver(),args);
  }

  public static Job prepareJob(Configuration conf,Path inputPath,Path dictRootPath) throws IOException {
    FileSystemUtil.setCombineInputSplitSize(conf,inputPath);
    conf.set(DICT_ROOT,dictRootPath.toString());
    Path dictOutputPath=new Path(dictRootPath,"updateDictOut_"+inputPath.getName().substring(0,inputPath.getName().length()-1));
    FileSystem fs=FileSystem.get(conf);
    if(fs.exists(dictOutputPath))
      fs.delete(dictOutputPath,true);
    Job updateDictJob=new Job(conf);
    updateDictJob.setInputFormatClass(CombineTextInputFormat.class);
    updateDictJob.setMapperClass(UpdateDictMapper.class);
    updateDictJob.setReducerClass(UpdateDictReducer.class);
    FileInputFormat.addInputPath(updateDictJob, inputPath);
    TextOutputFormat.setOutputPath(updateDictJob, dictOutputPath);
    updateDictJob.setOutputFormatClass(TextOutputFormat.class);
    updateDictJob.setMapOutputKeyClass(Text.class);
    updateDictJob.setMapOutputValueClass(IntWritable.class);
    updateDictJob.setOutputKeyClass(Text.class);
    updateDictJob.setOutputValueClass(IntWritable.class);
    updateDictJob.setJarByClass(UpdateDictDriver.class);
    updateDictJob.setJobName("update Dict");
    return updateDictJob;
  }
}
