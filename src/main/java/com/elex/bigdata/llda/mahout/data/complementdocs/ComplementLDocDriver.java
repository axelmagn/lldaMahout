package com.elex.bigdata.llda.mahout.data.complementdocs;

import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver;
import com.elex.bigdata.llda.mahout.dictionary.UpdateDictDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.MultiLabelVectorWritable;

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 10:57 AM
 * To change this template use File | Settings | File Templates.
 */
public class ComplementLDocDriver extends AbstractJob {
  /*
      leftInputPaths: lDocs to complement the docs
      rightInputPath: lDocs which will be complemented
      rightKeyPath; uidFile
      OutputPath: completeLDocs
      ComplementLDocMapper:
         extract uid from uidFile;
         write uid and labeledDocument to reducer (uid in uidFile)
      ComplementLDocReducer:
         merge labeledDocuments with same uid
         write them to hdfs
  */
  public static final String PRE_LDOC_OPTION_NAME="leftInput";
  public static final String DOC_INF_DIR="inf";
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOption(PRE_LDOC_OPTION_NAME,"li","previous lDocs");
    addOption(GenerateLDocDriver.DOC_ROOT,"docsRoot","docs root directory");
    addOption(UpdateDictDriver.DICT_OPTION_NAME,"dictRoot","dictionary root path");
    if(parseArguments(args)==null){
      return -1;
    }
    Path inputPath=getInputPath();
    String docsRoot=getOption(GenerateLDocDriver.DOC_ROOT);
    String leftDir=getOption(PRE_LDOC_OPTION_NAME);
    Path leftInputPath=new Path(docsRoot+ File.separator+leftDir);
    Path outputPath=new Path(docsRoot+File.separator+"inf");
    String uidFilePath=docsRoot+File.separator+"uid";
    Configuration conf=new Configuration();

    Job complementLDocJob=prepareJob(conf,new Path[]{leftInputPath,inputPath},outputPath,uidFilePath);
    complementLDocJob.submit();
    complementLDocJob.waitForCompletion(true);
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public static Job prepareJob(Configuration conf,Path[] inputPaths,Path outputPath,String uidFilePath) throws IOException {
    conf.set(GenerateLDocDriver.UID_PATH,uidFilePath);
    FileSystem fs=FileSystem.get(conf);
    if(fs.exists(outputPath))
      fs.delete(outputPath);
    Job job=new Job(conf);
    job.setNumReduceTasks(1);
    job.setMapperClass(ComplementLDocMapper.class);
    job.setReducerClass(ComplementLDocReducer.class);
    for(Path inputPath: inputPaths){
      FileInputFormat.addInputPath(job,inputPath);
    }
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MultiLabelVectorWritable.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MultiLabelVectorWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(job,outputPath);
    job.setJobName("complement docs");
    job.setJarByClass(ComplementLDocDriver.class);
    return job;
  }
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new ComplementLDocDriver(),args);
  }
}
