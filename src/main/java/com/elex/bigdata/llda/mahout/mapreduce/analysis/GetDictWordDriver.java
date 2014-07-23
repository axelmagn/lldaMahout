package com.elex.bigdata.llda.mahout.mapreduce.analysis;

import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver;
import com.elex.bigdata.llda.mahout.data.inputformat.CombineTextInputFormat;
import com.elex.bigdata.llda.mahout.dictionary.UpdateDictDriver;
import com.elex.bigdata.llda.mahout.dictionary.UpdateDictMapper;
import com.elex.bigdata.llda.mahout.util.FileSystemUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import static com.elex.bigdata.llda.mahout.dictionary.UpdateDictDriver.*;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/23/14
 * Time: 10:19 AM
 * To change this template use File | Settings | File Templates.
 */
public class GetDictWordDriver extends AbstractJob {

  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption(UpdateDictDriver.COUNT_THRESHOLD,"count_threshold","specify the word count threshold to add to dictionary",false);
    if (parseArguments(args) == null)
      return -1;
    inputPath = getInputPath();
    outputPath = getOutputPath();
    Configuration conf = getConf();
    conf.set(COUNT_THRESHOLD,getOption(COUNT_THRESHOLD,String.valueOf(DEFAULT_COUNT_THRESHOLD)));
    Job job = prepareJob(conf, inputPath, outputPath);
    job.submit();
    job.waitForCompletion(true);
    return 0;
  }
  private Job prepareJob(Configuration conf,Path inputPath,Path outputPath) throws IOException {
    FileSystemUtil.setCombineInputSplitSize(conf, inputPath);
    Job job=new Job(conf);
    FileSystem fs= FileSystem.get(conf);
    if(fs.exists(outputPath))
      fs.delete(outputPath);
    job.setMapperClass(UpdateDictMapper.class);
    job.setReducerClass(GetDictWordReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setInputFormatClass(CombineTextInputFormat.class);
    FileInputFormat.addInputPath(job, inputPath);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setJarByClass(GetDictWordDriver.class);
    job.setJobName("get dict word "+inputPath.toString());
    return job;
  }

  public static class GetDictWordReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private int word_count_threshold;
    public void setup(Context context){
      Configuration conf=context.getConfiguration();
      word_count_threshold=Integer.parseInt(conf.get(UpdateDictDriver.COUNT_THRESHOLD));
    }
    public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
      int wordCount=0;
      for(IntWritable countWritable:values){
        wordCount+=countWritable.get();
      }
      if(wordCount>=word_count_threshold&&wordCount<10000)
        context.write(key,new IntWritable(wordCount));
    }

  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new GenerateLDocDriver(),args);
  }
}
