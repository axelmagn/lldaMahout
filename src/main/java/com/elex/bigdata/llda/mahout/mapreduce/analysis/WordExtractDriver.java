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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/8/14
 * Time: 3:42 PM
 * To change this template use File | Settings | File Templates.
 */
public class WordExtractDriver extends AbstractJob{
  public static final int TOP_COUNT_WORD=10000;
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
    FileSystemUtil.setCombineInputSplitSize(conf, inputPath);
    Job job=new Job(conf);
    FileSystem fs= FileSystem.get(conf);
    if(fs.exists(outputPath))
      fs.delete(outputPath);
    job.setMapperClass(WordExtractMapper.class);
    job.setReducerClass(WordExtractReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setInputFormatClass(CombineTextInputFormat.class);
    FileInputFormat.addInputPath(job, inputPath);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setJarByClass(WordExtractDriver.class);
    job.setJobName("word extract  "+inputPath.toString());
    return job;
  }
  public static class WordExtractMapper extends Mapper<Object,Text,Text,IntWritable>{
    public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
      String[] tokens=value.toString().split("\t");
      if(tokens.length<3)
      {
        System.out.println("wrong line "+value.toString());
        return;
      }
      String shortWord=tokens[1];
      if(shortWord.startsWith("http://"))
        shortWord=shortWord.substring(7);
      else if(shortWord.startsWith("https://"))
        shortWord=shortWord.substring(8);
      if(shortWord.contains("/"))
        shortWord=shortWord.substring(0,shortWord.indexOf("/"));
      context.write(new Text(shortWord),new IntWritable(Integer.parseInt(tokens[2])));
    }
  }
  public static class WordExtractReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
    private PriorityQueue<WordCount> priorityQueue=new PriorityQueue<WordCount>(TOP_COUNT_WORD);
    public void reduce(Text key,Iterable<IntWritable> values,Context context){
      int count=0;
      Iterator<IntWritable> iter=values.iterator();
      while(iter.hasNext()){
         count+=iter.next().get();
      }
      WordCount wordCount=new WordCount(key.toString(),count);
      priorityQueue.add(wordCount);
      if(priorityQueue.size()>TOP_COUNT_WORD)
        priorityQueue.poll();
    }
    public void cleanup(Context context) throws IOException, InterruptedException {
      Stack<WordCount> stack=new Stack<WordCount>();
      while(!priorityQueue.isEmpty()){
        stack.add(priorityQueue.poll());
      }
      while(!stack.isEmpty()){
        WordCount wordCount=stack.pop();
        context.write(new Text(wordCount.getWord()),new IntWritable(wordCount.getCount()));
      }
    }
  }

  public static class WordCount implements Comparable {
    private String word;
    private int count;
    public WordCount(String word,int count){
      this.word=word;
      this.count=count;
    }

    public String getWord() {
      return word;
    }

    public int getCount() {
      return count;
    }

    @Override
    public int compareTo(Object o) {
      return count-((WordCount)o).getCount();
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new WordExtractDriver(),args);
  }
}
