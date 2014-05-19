package com.elex.bigdata.llda.mahout.data.mergedocs;

import com.elex.bigdata.llda.mahout.data.LabeledDocumentWritable;
import com.elex.bigdata.llda.mahout.data.complementdocs.ComplementLDocDriver;
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

import java.io.File;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 10:12 AM
 * To change this template use File | Settings | File Templates.
 */
public class MergeLDocDriver extends AbstractJob {
  /*
     InputPaths: labeledDocs files
     conf.setDictSizePath
     MergeLDocMapper:
        create a new labeledDocument with size of dictSize and clone from value
     MergeLDocReducer:
        merge labels and urlCounts for uid:Iterable<LabeledDocument>
        context.write(uid,labeledDocument)
   */
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOption(ComplementLDocDriver.PRE_LDOC_OPTION_NAME,"li","previous lDocs");
    addOption(GenerateLDocDriver.DOC_ROOT_OPTION_NAME,"docsRoot","docs root directory");
    addOption(UpdateDictDriver.DICT_OPTION_NAME,"dictRoot","dictionary root path");
    if(parseArguments(args)==null){
      return -1;
    }
    Path inputPath=getInputPath();
    String docsRoot=getOption(GenerateLDocDriver.DOC_ROOT_OPTION_NAME);
    String leftDir=getOption(ComplementLDocDriver.PRE_LDOC_OPTION_NAME);
    Path leftInputPath=new Path(docsRoot+ File.separator+leftDir);
    Path outputPath=new Path(docsRoot+File.separator+"est");
    String dictRoot=getOption("dictionary");
    Configuration conf=new Configuration();

    Job complementLDocJob=prepareJob(conf,new Path[]{leftInputPath,inputPath},outputPath,dictRoot);
    complementLDocJob.submit();
    complementLDocJob.waitForCompletion(true);
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public static Job prepareJob(Configuration conf,Path[] inputPaths,Path outputPath,String dictRoot) throws IOException {
    conf.set(UpdateDictDriver.DICT_SIZE_PATH,dictRoot+ File.separator+"dictSize");
    FileSystem fs=FileSystem.get(conf);
    if(fs.exists(outputPath))
      fs.delete(outputPath);
    Job job=new Job(conf);
    job.setMapperClass(MergeLDocMapper.class);
    job.setReducerClass(MergeLDocReducer.class);
    for(Path inputPath: inputPaths){
      FileInputFormat.addInputPath(job, inputPath);
    }
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LabeledDocumentWritable.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LabeledDocumentWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(job,outputPath);
    job.setJobName("merge docs");
    job.setJarByClass(MergeLDocDriver.class);
    return job;
  }
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new MergeLDocDriver(), args);
  }
}
