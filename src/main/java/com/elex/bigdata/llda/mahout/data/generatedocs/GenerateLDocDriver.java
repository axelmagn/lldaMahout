package com.elex.bigdata.llda.mahout.data.generatedocs;

import com.elex.bigdata.llda.mahout.data.inputformat.CombineTextInputFormat;
import com.elex.bigdata.llda.mahout.dictionary.UpdateDictDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.MultiLabelVectorWritable;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 3:50 PM
 * To change this template use File | Settings | File Templates.
 */
public class GenerateLDocDriver extends AbstractJob {

  public static final String UID_PATH="uid_path";
  public static final String RESOURCE_ROOT ="resource_root";
  public static final String UID_FILE="uid";
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOption(UpdateDictDriver.DICT_ROOT,"dict","dictionary root Path",true);
    addOption(RESOURCE_ROOT,"rDir","specify the resources Dir",true);
    addOption(UID_PATH,"uid_path","specify the file's path which save uids in the input file",true);
    addOutputOption();

    if(parseArguments(args)==null){
      return -1;
    }
    inputPath=getInputPath();
    Path dictRootPath=new Path(getOption(UpdateDictDriver.DICT_ROOT));
    Path uidPath=new Path(getOption(UID_PATH));
    Path resourcesPath=new Path(getOption(RESOURCE_ROOT));
    outputPath=getOutputPath();
    Configuration conf=new Configuration();
    FileSystem fs= FileSystem.get(conf);
    if(fs.exists(outputPath))
      fs.delete(outputPath);
    Job genLDocJob=prepareJob(conf,inputPath,dictRootPath,resourcesPath,outputPath,uidPath);
    genLDocJob.submit();
    genLDocJob.waitForCompletion(true);
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public static Job prepareJob(Configuration conf,Path inputPath,Path dictRootPath,Path resourcesPath,Path outputPath,Path uidFilePath) throws IOException {
    conf.setLong("mapred.max.split.size", 100*1000*1000); // 100m
    conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 100*1000*1000);
    conf.set(UID_PATH, uidFilePath.toString());
    conf.set(UpdateDictDriver.DICT_ROOT,dictRootPath.toString());
    conf.set(RESOURCE_ROOT,resourcesPath.toString());
    FileSystem fs=FileSystem.get(conf);
    if(fs.exists(outputPath))
      fs.delete(outputPath);
    Job genLDocJob=new Job(conf);
    genLDocJob.setNumReduceTasks(1);
    genLDocJob.setMapperClass(GenerateLDocMapper.class);
    genLDocJob.setReducerClass(GenerateLDocReducer.class);
    genLDocJob.setMapOutputKeyClass(Text.class);
    genLDocJob.setMapOutputValueClass(Text.class);
    genLDocJob.setInputFormatClass(CombineTextInputFormat.class);
    CombineTextInputFormat.addInputPath(genLDocJob, inputPath);
    SequenceFileOutputFormat.setOutputPath(genLDocJob, outputPath);
    genLDocJob.setOutputFormatClass(SequenceFileOutputFormat.class);
    genLDocJob.setOutputKeyClass(Text.class);
    genLDocJob.setOutputValueClass(MultiLabelVectorWritable.class);
    genLDocJob.setJarByClass(GenerateLDocDriver.class);
    genLDocJob.setJobName("generate LDocs");
    return genLDocJob;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new GenerateLDocDriver(),args);
  }

}
