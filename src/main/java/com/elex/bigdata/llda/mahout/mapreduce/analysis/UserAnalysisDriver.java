package com.elex.bigdata.llda.mahout.mapreduce.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/2/14
 * Time: 3:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class UserAnalysisDriver extends AbstractJob{

  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    if(parseArguments(args)==null)
      return -1;
    inputPath=getInputPath();
    outputPath=getOutputPath();
    Configuration conf=getConf();
    Job job=prepareJob(conf,inputPath,outputPath);
    job.submit();
    job.waitForCompletion(true);
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }
  private Job prepareJob(Configuration conf,Path inputPath,Path outputPath) throws IOException {
    Job job=new Job(conf);
    FileSystem fs= FileSystem.get(conf);
    if(fs.exists(outputPath))
      fs.delete(outputPath);
    job.setMapperClass(UserAnalysisMapper.class);
    job.setReducerClass(UserAnalysisReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    SequenceFileInputFormat.addInputPath(job, inputPath);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job,outputPath);
    job.setJarByClass(UserAnalysisDriver.class);
    job.setJobName("analysis");
    return job;
  }
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new UserAnalysisDriver(),args);
  }
}
