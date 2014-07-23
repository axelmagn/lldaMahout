package com.elex.bigdata.llda.mahout.mapreduce.analysis;

import com.elex.bigdata.llda.mahout.data.inputformat.CombineTextInputFormat;
import com.elex.bigdata.llda.mahout.dictionary.UpdateDictMapper;
import com.elex.bigdata.llda.mahout.util.FileSystemUtil;
import com.elex.bigdata.llda.mahout.util.Trie;
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
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/23/14
 * Time: 3:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class WordCleanDriver extends AbstractJob {
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
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  private Job prepareJob(Configuration conf,Path inputPath,Path outputPath) throws IOException {
    FileSystemUtil.setCombineInputSplitSize(conf, inputPath);
    Job job=new Job(conf);
    FileSystem fs= FileSystem.get(conf);
    if(fs.exists(outputPath))
      fs.delete(outputPath,true);
    job.setMapperClass(WordCleanMapper.class);
    job.setReducerClass(WordCleanReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setInputFormatClass(CombineTextInputFormat.class);
    job.setNumReduceTasks(4);
    FileInputFormat.addInputPath(job, inputPath);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setJarByClass(WordCleanDriver.class);
    job.setJobName("word clean "+inputPath.toString());
    return job;
  }

  public static class WordCleanMapper extends Mapper<Object,Text,Text,Text> {
    public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
      String[] uidUrlCount = value.toString().split("\t");
      if (uidUrlCount.length < 3) {
        System.out.println("wrong uidUrlCount " + value.toString());
        return;
      }
      String url = uidUrlCount[1];
      int index = url.indexOf('?');
      if (index != -1)
        url = url.substring(0, index);
      int frequent = 0;
      for (int i = 0; i < url.length(); i++) {
        if (url.charAt(i) == '/') {
          frequent++;
          if (frequent == 3) {
            url = url.substring(0, i);
            break;
          }
        }
      }
      context.write(new Text(uidUrlCount[0]),new Text(url+"\t"+uidUrlCount[2]));
    }
  }

  public static class WordCleanReducer extends Reducer<Text,Text,Text,Text> {
    private int uidNum=0;
    private long trieCost=0;
    public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
      uidNum++;
      long t1=System.nanoTime();
      Trie trie=new Trie();
      for(Text wordCount: values){
        String[] tokens=wordCount.toString().split("\t");
        trie.insert(tokens[0],Integer.parseInt(tokens[1]));
      }
      Map<String,Integer> wordCountMap=trie.searchCommonStr('/');
      trieCost+=System.nanoTime()-t1;
      for(Map.Entry<String,Integer> entry: wordCountMap.entrySet()){
        context.write(key,new Text(entry.getKey()+"\t"+entry.getValue()));
      }
      if(uidNum%10000==1){
        System.out.println("trieCost: "+trieCost/1000);
      }
    }
    public void cleanup(Context context){
      System.out.println("trieCost: "+trieCost/1000);
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new WordCleanDriver(),args);
  }


}
