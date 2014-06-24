package com.elex.bigdata.llda.mahout.data.mergedocs;

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
import org.apache.mahout.math.MultiLabelVectorWritable;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 10:12 AM
 * To change this template use File | Settings | File Templates.
 */
public class MergeLDocDriver extends AbstractJob {
  public static final String MULTI_INPUT="multi_input";
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
    addOption(MULTI_INPUT, "mI", "specify the input Path", true);
    addOutputOption();
    if(parseArguments(args)==null){
      return -1;
    }
    String multiInputs=getOption(MULTI_INPUT);
    String[] inputs=multiInputs.split(":");
    Path[] inputPaths=new Path[inputs.length];
    for(int i=0;i<inputs.length;i++)
       inputPaths[i]=new Path(inputs[i]);
    outputPath=getOutputPath();
    Configuration conf=new Configuration();

    Job mergeLDocJob=prepareJob(conf,inputPaths,outputPath);
    mergeLDocJob.submit();
    mergeLDocJob.waitForCompletion(true);
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public static Job prepareJob(Configuration conf,Path[] inputPaths,Path outputPath) throws IOException {
    FileSystem fs=FileSystem.get(conf);
    if(fs.exists(outputPath))
      fs.delete(outputPath);
    Job job=new Job(conf);
    job.setNumReduceTasks(1);
    job.setMapperClass(MergeLDocMapper.class);
    job.setReducerClass(MergeLDocReducer.class);
    for(Path inputPath: inputPaths){
      FileInputFormat.addInputPath(job, inputPath);
    }
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MultiLabelVectorWritable.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MultiLabelVectorWritable.class);
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
