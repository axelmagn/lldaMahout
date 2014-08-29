package com.elex.bigdata.llda.mahout.mapreduce.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.mahout.common.AbstractJob;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/29/14
 * Time: 4:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class LappedWordDriver extends AbstractJob{
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    if(parseArguments(args)==null)
      return -1;
    Job job=prepareJob(getConf(),getInputPath(),getOutputPath());
    job.submit();
    job.waitForCompletion(true);
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public Job prepareJob(Configuration conf,Path inputPath,Path outputPath) throws IOException {
    Job job=new Job(conf,"lapped Word "+inputPath.toString());
    job.setMapperClass(LappedWordMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setNumReduceTasks(0);
    FileInputFormat.addInputPath(job,inputPath);
    TextOutputFormat.setOutputPath(job, outputPath);
    job.setJarByClass(LappedWordDriver.class);
    return job;
  }

  private static  class LappedWordMapper extends Mapper<Object,Text,Text,NullWritable> {
     BloomFilter bloomFilter=new BloomFilter(1000*1000,3,0);
     public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
       String[] tokens=value.toString().split("\t");
       if(bloomFilter.membershipTest(new Key(Bytes.toBytes(tokens[0]))))
         context.write(new Text(tokens[0]), NullWritable.get());
       else
         bloomFilter.add(new Key(Bytes.toBytes(tokens[0])));
     }
  }
}
