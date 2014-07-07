package com.elex.bigdata.llda.mahout.mapreduce.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/7/14
 * Time: 9:49 AM
 * To change this template use File | Settings | File Templates.
 */
public class WordUniqDriver {
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
     String inputPath=args[0];
     String outputPath=args[1];
     Configuration conf=new Configuration();
     Job job=prepareJob(conf,new Path(inputPath),new Path(outputPath));
     job.submit();
     job.waitForCompletion(true);
  }

  public static Job prepareJob(Configuration conf, Path inputPath, Path outputPath) throws IOException {
    Job job = new Job(conf);
    job.setMapperClass(WordUniqMapper.class);
    job.setReducerClass(WordUniqReducer.class);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.addInputPath(job, inputPath);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    FileSystem fs= FileSystem.get(conf);
    if(fs.exists(outputPath))
      fs.delete(outputPath,true);
    TextOutputFormat.setOutputPath(job, outputPath);
    job.setJobName("word uniq " + inputPath.toString());
    job.setJarByClass(WordUniqDriver.class);
    return job;
  }

  public static class WordUniqReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
      context.write(key, NullWritable.get());

    }
  }

  public static class WordUniqMapper extends Mapper<Object, Text, Text, NullWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] tokens = line.split("\t");
      if (tokens.length < 3)
        return;
      context.write(new Text(tokens[1]), NullWritable.get());
    }
  }
}
