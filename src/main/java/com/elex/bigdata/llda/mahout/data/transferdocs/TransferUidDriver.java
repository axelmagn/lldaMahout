package com.elex.bigdata.llda.mahout.data.transferdocs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.MultiLabelVectorWritable;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/14/14
 * Time: 5:14 PM
 * To change this template use File | Settings | File Templates.
 */
public class TransferUidDriver extends AbstractJob{

  public static void runJob(Configuration conf,Path inputPath,Path outputPath) throws InterruptedException, IOException, ClassNotFoundException {
    Job job=new TransferUidDriver().prepareJob(conf,inputPath,outputPath);
    job.submit();
    job.waitForCompletion(true);
    return;
  }
  public Job prepareJob(Configuration conf,Path inputPath,Path outputPath) throws IOException {
    Job job=new Job(conf,"transfer uid "+inputPath.getName());
    job.setJarByClass(TransferUidDriver.class);
    job.setMapperClass(TransferUidMapper.class);
    job.setMapOutputValueClass(MultiLabelVectorWritable.class);
    job.setMapOutputKeyClass(Text.class);
    job.setReducerClass(Reducer.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    SequenceFileInputFormat.addInputPath(job,inputPath);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(job,outputPath);
    return job;
  }

  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    if(parseArguments(args)==null)
      return -1;
    Job job=prepareJob(getConf(),inputPath,outputPath);
    job.submit();
    job.waitForCompletion(true);
    return 0;
  }

  public static  void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new TransferUidDriver(),args);
  }
}
