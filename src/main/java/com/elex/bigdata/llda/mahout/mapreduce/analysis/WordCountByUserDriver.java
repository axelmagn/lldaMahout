package com.elex.bigdata.llda.mahout.mapreduce.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
 * Date: 7/2/14
 * Time: 3:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class WordCountByUserDriver extends AbstractJob{

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
    job.setMapperClass(WordCountByUserMapper.class);
    job.setReducerClass(WordCountByUserReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    SequenceFileInputFormat.addInputPath(job, inputPath);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job,outputPath);
    job.setJarByClass(WordCountByUserDriver.class);
    job.setJobName("analysis");
    return job;
  }
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new WordCountByUserDriver(),args);
  }
  public static class WordCountByUserMapper extends Mapper<Text,MultiLabelVectorWritable,Text,IntWritable> {
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
  public static class WordCountByUserReducer extends Reducer<Text,IntWritable,Text,Text> {
    private int[] thresholds=new int[]{2,4,8,16,32,64,128,256,512,1024,2048,4096,9192,18384};
    private int[] counts=new int[thresholds.length+1];
    private int allCount=0;
    public void setup(Context context){
      for(int i=0;i<counts.length;i++){
        counts[i]=0;
      }
    }
    public void reduce(Text key,Iterable<IntWritable> values,Context context){
      int count=0;
      Iterator<IntWritable> iterator=values.iterator();
      while(iterator.hasNext()){
        count+=iterator.next().get();
      }
      int i;
      for(i=0;i<thresholds.length;i++){
        if(count<=thresholds[i])
          break;
      }
      counts[i]+=1;
      allCount+=1;
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
      Text key=new Text("0~"+thresholds[0]);
      Text value=new Text(String.valueOf(counts[0]));
      context.write(key,value);
      for(int i=1;i<thresholds.length;i++){
        key=new Text(thresholds[i-1]+"~"+thresholds[i]);
        value=new Text(String.valueOf(counts[i]));
        context.write(key,value);
      }
      context.write(new Text(thresholds[thresholds.length-1]+"~"),new Text(String.valueOf(counts[counts.length-1])));
      context.write(new Text("all"),new Text(String.valueOf(allCount)));
    }
  }

}
