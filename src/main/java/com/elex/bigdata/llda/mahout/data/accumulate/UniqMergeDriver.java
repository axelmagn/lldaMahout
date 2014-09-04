package com.elex.bigdata.llda.mahout.data.accumulate;

import com.elex.bigdata.llda.mahout.data.inputformat.CombineTextInputFormat;
import com.elex.bigdata.llda.mahout.util.FileSystemUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import java.io.IOException;


/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/2/14
 * Time: 5:32 PM
 * To change this template use File | Settings | File Templates.
 */
public class UniqMergeDriver extends AbstractJob{
  public static final String MULTI_INPUT="multi_input";
  @Override
  public int run(String[] args) throws Exception {
    addOption(MULTI_INPUT, "mI", "specify the input Path", true);
    addOutputOption();
    if(parseArguments(args)==null)
      return -1;
    String multiInputs=getOption(MULTI_INPUT);
    String[] inputs=multiInputs.split(":");
    Path[] inputPaths=new Path[inputs.length];
    for(int i=0;i<inputs.length;i++)
      inputPaths[i]=new Path(inputs[i]);
    Job job=prepareJob(getConf(),inputPaths,getOutputPath());
    job.submit();
    job.waitForCompletion(true);
    return 0;
  }

  public Job prepareJob(Configuration conf,Path[] inputPaths,Path outputPath) throws IOException {
    FileSystemUtil.setCombineInputSplitSize(conf,inputPaths);
    FileSystemUtil.deleteOutputPath(conf,outputPath);
    Job job=new Job(conf,"UniqMerge ");
    job.setMapperClass(UniqMergeMapper.class);
    job.setInputFormatClass(CombineTextInputFormat.class);
    for(Path inputPath: inputPaths){
      FileInputFormat.addInputPath(job,inputPath);
    }
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);
    job.setReducerClass(UniqMergeReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    TextOutputFormat.setOutputPath(job,outputPath);
    job.setJarByClass(UniqMergeDriver.class);
    return job;

  }

  public class UniqMergeMapper extends Mapper<Object,Text,Text,NullWritable> {
    public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
      String[] tokens=value.toString().split("\t");
      context.write(new Text(tokens[0] + "\t" + tokens[1]), NullWritable.get());
    }
  }

  public class UniqMergeReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
    public void reduce(Text key,Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
      context.write(key, NullWritable.get());
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new UniqMergeDriver(),args);
  }
}
