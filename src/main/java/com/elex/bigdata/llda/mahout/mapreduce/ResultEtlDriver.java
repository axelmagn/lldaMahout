package com.elex.bigdata.llda.mahout.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.mahout.common.AbstractJob;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/28/14
 * Time: 11:17 AM
 * To change this template use File | Settings | File Templates.
 */
public class ResultEtlDriver extends AbstractJob{
  private static String LOCAL_RESULT_PAHT="localResultPath";

  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption(LOCAL_RESULT_PAHT,"lrp","local result output path","/data/log/user_category_result/pr");
    if(parseArguments(args)==null)
      return -1;
    Date date=new Date();
    DateFormat dateFormat=new SimpleDateFormat("yyyyMMdd");
    String day=dateFormat.format(date);
    int hour=date.getHours();
    int index=date.getMinutes()/5;
    Path inputPath=getInputPath();
    Path outputPath=getOutputPath();
    String localResultPath=getOption(LOCAL_RESULT_PAHT);
    if(localResultPath.endsWith("/"))
      localResultPath=localResultPath.substring(0,localResultPath.length()-1);
    Job etlJob=prepareJob(getConf(),inputPath,outputPath);
    etlJob.waitForCompletion(true);
    Runtime.getRuntime().exec("hadoop fs -getmerge "+outputPath.toString()+" "+localResultPath+ File.separator+"result."+day+"."+hour+"."+index);
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }
  private static Job prepareJob(Configuration conf,Path inputPath,Path outputPath) throws IOException, ClassNotFoundException, InterruptedException {
    Job job=new Job(conf);
    FileSystem fs=FileSystem.get(conf);
    if(fs.exists(outputPath))
      fs.delete(inputPath);
    job.setMapperClass(ResultEtlMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setNumReduceTasks(0);
    job.submit();
    return job;
  }
}
