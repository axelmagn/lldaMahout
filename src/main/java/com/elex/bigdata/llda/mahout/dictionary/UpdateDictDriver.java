package com.elex.bigdata.llda.mahout.dictionary;

import com.elex.bigdata.llda.mahout.data.preparedocs.PrepareInfDocsDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 11:45 AM
 * To change this template use File | Settings | File Templates.
 */
public class UpdateDictDriver extends AbstractJob{
  public static final String DICT_PATH="dict_path";
  public static final String DICT_SIZE_PATH="dict_size_path";
  public static final String TMP_DICT_PATH="tmp_dict_path";
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOption(PrepareInfDocsDriver.DICT_OPTION_NAME,"dict","dictionary root Path");
    if(parseArguments(args)==null)
      return -1;
    Path textInputPath=getInputPath();
    String dictRoot=getOption(PrepareInfDocsDriver.DICT_OPTION_NAME);
    String dictPath=dictRoot+ File.separator+"dict";
    String tmpDictPath=dictRoot+File.separator+"tmpDict";
    String dictSizePath=dictRoot+File.separator+"dictSize";
    Configuration conf=getConf();
    conf.set(UpdateDictDriver.DICT_PATH,dictPath);
    conf.set(UpdateDictDriver.DICT_SIZE_PATH,dictSizePath);
    conf.set(UpdateDictDriver.TMP_DICT_PATH,tmpDictPath);
    setConf(conf);
    Path dictOutputPath=new Path(dictRoot+File.separator+"updateDictOut");
    FileSystem fs=FileSystem.get(conf);
    if(fs.exists(dictOutputPath))
      fs.delete(dictOutputPath);
    Job updateDictJob=prepareJob(textInputPath,dictOutputPath, FileInputFormat.class, UpdateDictMapper.class, LongWritable.class,Text.class, UpdateDictReducer.class,Text.class, IntWritable.class, SequenceFileOutputFormat.class);
    updateDictJob.submit();
    updateDictJob.waitForCompletion(true);
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new UpdateDictDriver(),args);
  }
}
