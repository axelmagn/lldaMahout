package com.elex.bigdata.llda.mahout.mapreduce.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.Arrays;


/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/11/14
 * Time: 10:09 AM
 * To change this template use File | Settings | File Templates.
 */
public class TopicTermModelDriver extends AbstractJob {
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    Job job=prepareJob(getConf(),getInputPath(),getOutputPath());
    job.submit();
    job.waitForCompletion(true);
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }
  public Job prepareJob(Configuration conf,Path inputPath,Path outputPath) throws IOException {
    FileSystem fs=FileSystem.get(conf);
    if(fs.exists(outputPath))
      fs.delete(outputPath,true);
    Job job=new Job(conf,"topic term model analysis "+inputPath.toString());
    job.setMapperClass(TopicTermModelMapper.class);
    job.setReducerClass(Reducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    FileInputFormat.addInputPath(job,inputPath);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job,outputPath);
    job.setJarByClass(TopicTermModelDriver.class);
    return job;
  }
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new TopicTermModelDriver(),args);
  }
  public static class TopicTermModelMapper extends Mapper<IntWritable,VectorWritable,Text,Text>{
    private double[] ratio = new double[]{0.0,0.001,0.01,0.1,1,10,100,Double.MAX_VALUE};
    private int[] termCounts=new int[ratio.length-1];
    public void setup(Context context){
      Arrays.fill(termCounts, 0);
    }
    public void map(IntWritable key,VectorWritable value,Context context) throws IOException, InterruptedException {
      Vector probVector=value.get();
      double averageProb=1/(double)probVector.size();
      for(Vector.Element e: probVector){
         double prob=e.get();
         int i=1;
         for(;i<ratio.length-1;i++){
            if(prob<ratio[i]*averageProb)
              break;
         }
         termCounts[i]+=1;
      }
      StringBuilder builder=new StringBuilder();
      for(int j=1;j<ratio.length;j++){
         builder.append(ratio[j-1]+"~"+ratio[j]+" : "+termCounts[j-1]+"\t");
      }
      context.write(new Text(String.valueOf(key.get())),new Text(builder.toString()));
    }
  }
}
