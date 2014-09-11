package com.elex.bigdata.llda.mahout.mapreduce.analysis.word;

import com.elex.bigdata.llda.mahout.data.inputformat.CombineTextInputFormat;
import com.elex.bigdata.llda.mahout.util.FileSystemUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/10/14
 * Time: 3:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class IdfDriver extends AbstractJob {
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    if(parseArguments(args)==null)
      return -1;
    Job job=prepareJob(getConf(),getInputPath(),getOutputPath());
    job.submit();
    job.waitForCompletion(true);
    return 0;
  }

  public Job prepareJob(Configuration conf,Path inputPath,Path outputPath) throws IOException {
    FileSystemUtil.deleteOutputPath(conf,outputPath);
    FileSystemUtil.setCombineInputSplitSize(conf,inputPath);
    Job job=new Job(conf,"IdfDriver");
    job.setMapperClass(IdfMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setReducerClass(IdfReducer.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setInputFormatClass(CombineTextInputFormat.class);
    FileInputFormat.addInputPath(job, inputPath);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job,outputPath);
    job.setJarByClass(IdfDriver.class);
    return job;
  }

  public static class IdfMapper extends Mapper<Object,Text,Text,Text> {
    public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
      String[] tokens=value.toString().split("\t");
      if(tokens.length<2)
        return;
      context.write(new Text(tokens[1]),new Text(tokens[0]));
    }

  }

  public static class IdfReducer extends Reducer<Text,Text,Text,DoubleWritable> {
    private double numSum=100*10000;
    public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
      Set<String> uids=new HashSet<String>();
      for(Text value: values)
        uids.add(value.toString());
      context.write(key,new DoubleWritable(Math.log(numSum/uids.size())));
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new IdfDriver(),args);
  }
}
