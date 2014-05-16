package com.elex.bigdata.llda.mahout.data.generatedocs;

import com.elex.bigdata.llda.mahout.data.LabeledDocumentWritable;
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

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 3:50 PM
 * To change this template use File | Settings | File Templates.
 */
public class GenerateLDocDriver extends AbstractJob {
  public static final String URL_CATEGORY_PATH="url_category_path";
  public static final String CATEGORY_LABEL_PATH="category_label_path";
  public static final String UID_PATH="uid_path";
  public static final String DOC_ROOT_OPTION_NAME="docsRoot";
  public static final String DOC_OPTION_NAME="docsDir";
  public static final String RESOURCE_OPTION_NAME="resourceDir";
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOption(DOC_ROOT_OPTION_NAME,"docsRoot","specify the lDocs Root Directory");
    addOption(DOC_OPTION_NAME,"docsDir","specify the lDocs directory");
    addOption(UpdateDictDriver.DICT_OPTION_NAME,"dict","dictionary root Path",true);
    addOption(RESOURCE_OPTION_NAME,"rDir","specify the resources Dir");
    if(parseArguments(args)==null){
      return -1;
    }
    Path inputPath=getInputPath();
    String docsRoot=getOption(DOC_ROOT_OPTION_NAME);
    String docsPath=docsRoot+File.separator+getOption(DOC_OPTION_NAME);
    String dictRoot=getOption(UpdateDictDriver.DICT_OPTION_NAME);
    String resourcesDir=getOption(RESOURCE_OPTION_NAME);
    String uidPath=docsRoot+File.separator+"uid";
    Configuration conf=new Configuration();
    FileSystem fs= FileSystem.get(conf);
    if(fs.exists(new Path(docsPath)))
      fs.delete(new Path(docsPath));
    Job genLDocJob=prepareJob(conf,inputPath,new Path(docsPath),dictRoot,resourcesDir,uidPath);
    genLDocJob.submit();
    genLDocJob.waitForCompletion(true);
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public static Job prepareJob(Configuration conf,Path inputPath,Path outputPath,String dictRoot,String resourcesDir,String uidFilePath) throws IOException {
    String urlCategoryPath=resourcesDir+ File.separator+"url_category";
    String categoryLabelPath=resourcesDir+File.separator+"category_label";
    conf.setLong("mapred.max.split.size", 22485760); // 10m
    conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 22485760);
    conf.set(URL_CATEGORY_PATH,urlCategoryPath);
    conf.set(CATEGORY_LABEL_PATH,categoryLabelPath);
    conf.set(UID_PATH,uidFilePath);
    conf.set(UpdateDictDriver.DICT_PATH,dictRoot+File.separator+"dict");
    FileSystem fs=FileSystem.get(conf);
    if(fs.exists(outputPath))
      fs.delete(outputPath);
    Job genLDocJob=new Job(conf);
    genLDocJob.setMapperClass(GenerateLDocMapper.class);
    genLDocJob.setReducerClass(GenerateLDocReducer.class);
    genLDocJob.setMapOutputKeyClass(Text.class);
    genLDocJob.setMapOutputValueClass(Text.class);
    genLDocJob.setInputFormatClass(CombineTextInputFormat.class);
    CombineTextInputFormat.addInputPath(genLDocJob, inputPath);
    SequenceFileOutputFormat.setOutputPath(genLDocJob, outputPath);
    genLDocJob.setOutputFormatClass(SequenceFileOutputFormat.class);
    genLDocJob.setOutputKeyClass(Text.class);
    genLDocJob.setOutputValueClass(LabeledDocumentWritable.class);
    genLDocJob.setJarByClass(GenerateLDocDriver.class);
    genLDocJob.setJobName("generate LDocs");
    return genLDocJob;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new GenerateLDocDriver(),args);
  }

}
