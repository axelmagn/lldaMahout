package com.elex.bigdata.llda.mahout.mapreduce.analysis.word;

import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver;
import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocReducer;
import com.elex.bigdata.llda.mahout.data.inputformat.CombineTextInputFormat;
import com.elex.bigdata.llda.mahout.util.FileSystemUtil;
import com.elex.bigdata.llda.mahout.util.PrefixTrie;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/14/14
 * Time: 10:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class PreClassifiedWordDriver extends AbstractJob {
  /**
   * 从计算出的uid-url-count数据中获取文件url_topic中存储的已经分好类的url及其相关url(即是存储的url的前缀）
   * 这个类需要更新，已经基本上不怎么使用了
   */
  @Override
  public int run(String[] args) throws Exception {
    addOutputOption();
    addInputOption();
    addOption(GenerateLDocDriver.RESOURCE_ROOT,"rDir","specify the resources Dir",true);
    if (parseArguments(args) == null)
      return -1;
    Configuration conf=getConf();
    conf.set(GenerateLDocDriver.RESOURCE_ROOT,getOption(GenerateLDocDriver.RESOURCE_ROOT));
    Job job = prepareJob(conf, getInputPath(), getOutputPath());
    job.submit();
    job.waitForCompletion(true);
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public Job prepareJob(Configuration conf, Path inputPath, Path outputPath) throws IOException {
    FileSystemUtil.setCombineInputSplitSize(conf, inputPath);
    FileSystemUtil.deleteOutputPath(conf, outputPath);
    Job job = new Job(conf,"hit Word Count "+inputPath.getName());
    job.setMapperClass(WordPrefixExtractDriver.WordExtractMapper.class);
    job.setReducerClass(HitWordCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setInputFormatClass(CombineTextInputFormat.class);
    FileInputFormat.addInputPath(job, inputPath);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setJarByClass(PreClassifiedWordDriver.class);
    return job;
  }

  public static class HitWordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Map<String, String> url_category_map = new HashMap<String, String>();
    PrefixTrie prefixTrie = new PrefixTrie();
    private String[] destCategories = new String[]{"jogos", "compras", "Friends"};
    Map<String, Integer> categoryIdMap = new HashMap<String, Integer>();
    Map<Integer, String> idCategoryMap = new HashMap<Integer, String>();
    private int hitWordCount=0,prefixHitCount=0, allWordCount=0;
    public void setup(Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      FileSystem fs = FileSystem.get(conf);
      Path resourcesPath = new Path(conf.get(GenerateLDocDriver.RESOURCE_ROOT));
      Path urlCategoryPath = new Path(resourcesPath, GenerateLDocReducer.URL_TOPIC);

      for (int i = 0; i < destCategories.length; i++) {
        categoryIdMap.put(destCategories[i], i);
        idCategoryMap.put(i, destCategories[i]);
      }
      BufferedReader urlCategoryReader = new BufferedReader(new InputStreamReader(fs.open(urlCategoryPath)));
      String line = "";
      while ((line = urlCategoryReader.readLine()) != null) {
        String[] categoryUrls = line.split(" ");
        //if (categoryIdMap.containsKey(categoryUrls[0])) {
          int id = 1;
          for (int i = 1; i < categoryUrls.length; i++)
            prefixTrie.insert(categoryUrls[i], id);
        //} else {
        //  for (int i = 1; i < categoryUrls.length; i++) {
        //    url_category_map.put(categoryUrls[i], categoryUrls[0]);
        //  }
        //}
      }
      urlCategoryReader.close();
      context.write(new Text("prefixSize "),new IntWritable(prefixTrie.getSize()));
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) {
       String word=key.toString();
       allWordCount++;
       if(url_category_map.containsKey(word)){
         hitWordCount++;
         return;
       }
       if(prefixTrie.prefixSearch(word)!=-1)
       {
         hitWordCount++;
         prefixHitCount++;
         return;
       }
    }
    public void cleanup(Context context) throws IOException, InterruptedException {
      context.write(new Text("hitWordCount"),new IntWritable(hitWordCount));
      context.write(new Text("prefixHitCount"),new IntWritable(prefixHitCount));
      context.write(new Text("allWordCount"),new IntWritable(allWordCount));
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new PreClassifiedWordDriver(), args);
  }
}
