package com.elex.bigdata.llda.mahout.dictionary;

import com.elex.bigdata.llda.mahout.data.inputformat.CombineTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
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
 * Date: 6/17/14
 * Time: 3:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class UpdateRegularDictDriver extends AbstractJob {
  public static final String DICT_PATH="dict_path";
  public static final String DICT_SIZE_PATH="dict_size_path";
  public static final String TMP_DICT_PATH="tmp_dict_path";
  public static final String DICT_OPTION_NAME="dictionary";
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOption(DICT_OPTION_NAME,"dict","dictionary root Path");
    if(parseArguments(args)==null)
      return -1;
    Path textInputPath=getInputPath();
    String dictRoot=getOption(DICT_OPTION_NAME);
    Configuration conf=new Configuration();
    Job updateDictJob=prepareJob(conf,textInputPath,dictRoot);
    updateDictJob.submit();
    updateDictJob.waitForCompletion(true);
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }
  public static void main(String[] args) throws Exception {
    System.out.println("hhhh");
    ToolRunner.run(new Configuration(), new UpdateRegularDictDriver(), args);

  }

  public static Job  prepareJob(Configuration conf,Path inputPath,String dictRoot) throws IOException {
    System.out.println("prepareJob");
    String dictPath=dictRoot+ File.separator+"dict";
    String tmpDictPath=dictRoot+File.separator+"tmpDict";
    String dictSizePath=dictRoot+File.separator+"dictSize";
    conf.setLong("mapred.max.split.size", 22485760); // 10m
    conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 22485760);
    conf.set(UpdateDictDriver.DICT_PATH,dictPath);
    conf.set(UpdateDictDriver.DICT_SIZE_PATH,dictSizePath);
    conf.set(UpdateDictDriver.TMP_DICT_PATH, tmpDictPath);
    Path dictOutputPath=new Path(dictRoot+File.separator+"updateDictOut");
    FileSystem fs=FileSystem.get(conf);
    if(fs.exists(dictOutputPath))
      fs.delete(dictOutputPath);

    Job updateDictJob=new Job(conf);
    updateDictJob.setInputFormatClass(CombineTextInputFormat.class);
    updateDictJob.setMapperClass(UpdateDictMapper.class);
    updateDictJob.setReducerClass(UpdateRegularDictReducer.class);
    FileInputFormat.addInputPath(updateDictJob, inputPath);
    SequenceFileOutputFormat.setOutputPath(updateDictJob, dictOutputPath);
    updateDictJob.setOutputFormatClass(SequenceFileOutputFormat.class);
    updateDictJob.setMapOutputKeyClass(Text.class);
    updateDictJob.setMapOutputValueClass(NullWritable.class);
    updateDictJob.setJarByClass(UpdateRegularDictDriver.class);
    updateDictJob.setJobName("Regular Dict update");
    System.out.println("set job name regular dict update");
    return updateDictJob;
  }
}
