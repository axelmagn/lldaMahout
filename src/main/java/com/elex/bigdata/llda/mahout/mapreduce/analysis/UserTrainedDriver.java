package com.elex.bigdata.llda.mahout.mapreduce.analysis;

import com.elex.bigdata.llda.mahout.data.inputformat.CombineTextInputFormat;
import com.elex.bigdata.llda.mahout.util.FileSystemUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.MultiLabelVectorWritable;
import org.apache.mahout.math.Vector;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/9/14
 * Time: 3:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class UserTrainedDriver extends AbstractJob{
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
    job.setMapperClass(UserTrainedMapper.class);
    job.setReducerClass(WordCountByUserDriver.WordCountByUserReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    SequenceFileInputFormat.addInputPath(job, inputPath);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setJarByClass(UserTrainedDriver.class);
    job.setJobName("user trained analysis "+inputPath.toString());
    return job;
  }
  public static class UserTrainedMapper extends Mapper<Text,MultiLabelVectorWritable,Text,IntWritable> {
    public void map(Text key,MultiLabelVectorWritable value,Context context) throws IOException, InterruptedException {
      Vector vector=value.getVector();
      Iterator<Vector.Element> iter=vector.iterateNonZero();
      int count=0;
      while(iter.hasNext()){
        Vector.Element e=iter.next();
        count+=e.get();
      }
      context.write(key,new IntWritable(count));
    }
  }
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new UserTrainedDriver(),args);
  }
}
