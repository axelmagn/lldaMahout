package com.elex.bigdata.llda.mahout.mapreduce.analysis;

import com.elex.bigdata.hashing.BDMD5;
import com.elex.bigdata.hashing.HashingException;
import com.elex.bigdata.llda.mahout.data.inputformat.CombineTextInputFormat;
import com.elex.bigdata.llda.mahout.util.FileSystemUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/7/14
 * Time: 6:10 PM
 * To change this template use File | Settings | File Templates.
 */
public class WordCountDriver extends AbstractJob {
  public static final BDMD5 bdmd5 = BDMD5.getInstance();

  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    if (parseArguments(args) == null)
      return -1;
    inputPath = getInputPath();
    outputPath = getOutputPath();
    Configuration conf = getConf();
    Job job = prepareJob(conf, inputPath, outputPath);
    job.submit();
    job.waitForCompletion(true);
    return 0;
  }
  private Job prepareJob(Configuration conf,Path inputPath,Path outputPath) throws IOException {
    FileSystemUtil.setCombineInputSplitSize(conf,inputPath);
    Job job=new Job(conf);
    FileSystem fs= FileSystem.get(conf);
    if(fs.exists(outputPath))
      fs.delete(outputPath);
    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setInputFormatClass(CombineTextInputFormat.class);
    FileInputFormat.addInputPath(job, inputPath);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setJarByClass(WordCountByUserDriver.class);
    job.setJobName("word count "+inputPath.toString());
    return job;
  }

  public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] tokens = line.split("\t");
      if (tokens.length < 3)
        return;
      try {
        context.write(new Text(bdmd5.toMD5(tokens[1])), new IntWritable(Integer.parseInt(tokens[2])));
      } catch (HashingException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
  }

  public static class WordCountReducer extends Reducer<Text, IntWritable, Text, Text> {
    private int[] thredhold = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 25, 30, 40, 60, 80, 100, 200, 400, 800, 1600, 3200, 6400};
    private int[] wordCount = new int[thredhold.length];
    private int totalCount=0;
    public void setup(Context context) {
      Arrays.fill(wordCount, 0);
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) {
      int count = 0;
      Iterator<IntWritable> iter = values.iterator();
      while (iter.hasNext()) {
        count += iter.next().get();
      }
      int i;
      for (i = 1; i < thredhold.length; i++) {
        if (count <= thredhold[i])
          break;
      }
      wordCount[i - 1] += 1;
      totalCount+=1;
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
      int i;
      context.write(new Text("totolCount"),new Text(String.valueOf(totalCount)));
      int count=0;
      for (i = 1; i < thredhold.length; i++) {
        count+=wordCount[i-1];
        context.write(new Text(thredhold[i - 1] + "~" + thredhold[i]+"\t"+wordCount[i - 1]), new Text("~"+thredhold[i]+"\t"+count));
      }
      context.write(new Text(thredhold[i-1]+"~"),new Text(String.valueOf(wordCount[i-1])));
    }
  }
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new WordCountDriver(),args);
  }
}
