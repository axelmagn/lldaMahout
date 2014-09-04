package com.elex.bigdata.llda.mahout.mapreduce.analysis;

import com.elex.bigdata.llda.mahout.util.FileSystemUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.mahout.common.AbstractJob;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/2/14
 * Time: 6:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class CategoryAnalyzeTool extends AbstractJob {
  public static final String CATEGORY_RESULT_DIR="category_result_dir";
  public static final String CATEGORY_FILE="userCategory";
  public static final String ANALYTIC_FILE="analytics";

  @Override
  public int run(String[] args) throws Exception {
    addOption(CATEGORY_RESULT_DIR,"crd","specify category result directory ",true);
    addInputOption();
    addOutputOption();
    if(parseArguments(args)==null)
      return -1;
    Configuration conf=getConf();
    conf.set(CATEGORY_RESULT_DIR,getOption(CATEGORY_RESULT_DIR));
    Job job=prepareJob(conf,inputPath,outputPath);
    job.submit();
    job.waitForCompletion(true);
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public Job prepareJob(Configuration conf,Path inputPath,Path outputPath) throws IOException {
    FileSystemUtil.deleteOutputPath(conf,outputPath);
    Job job=new Job(conf,"category analyze "+inputPath.toString());
    job.setMapperClass(CategoryAnalyzeMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setReducerClass(CategoryAnalyzeReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setJarByClass(CategoryAnalyzeTool.class);
    FileInputFormat.addInputPath(job,inputPath);
    TextOutputFormat.setOutputPath(job,outputPath);
    return job;
  }

  public static class CategoryAnalyzeMapper extends Mapper<Object,Text,Text,IntWritable>{
    private Map<Integer,BloomFilter> category2BloomFilter=new HashMap<Integer,BloomFilter>();
    private Map<Integer,Map<String,Integer>> categoryNation2Count=new HashMap<Integer, Map<String, Integer>>();
    public void setup(Context context) throws IOException {
      Configuration conf=context.getConfiguration();
      String categoryResultDir=conf.get(CATEGORY_RESULT_DIR);
      Path categoryFile=new Path(categoryResultDir,CATEGORY_FILE);
      Path analyticsFile=new Path(categoryResultDir,ANALYTIC_FILE);
      load(conf,categoryFile,analyticsFile);
    }
    private void load(Configuration conf,Path categoryFile,Path analyticsFile) throws IOException {
      FileSystem fs=FileSystem.get(conf);
      BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(analyticsFile)));
      String line;
      while((line=reader.readLine())!=null){
        String[] tokens=line.split(" ");
        category2BloomFilter.put(Integer.parseInt(tokens[0]),new BloomFilter(Integer.parseInt(tokens[1]),3,0));
      }
      reader.close();
      reader=new BufferedReader(new InputStreamReader(fs.open(categoryFile)));
      while((line=reader.readLine())!=null){
        String[] tokens=line.split("\t");
        category2BloomFilter.get(Integer.parseInt(tokens[1])).add(new Key(Bytes.toBytes(tokens[0])));
      }
    }
    public void map(Object key,Text value,Context context){
      String[] tokens=value.toString().split("\t");
      Integer category=Integer.parseInt(tokens[0]);
      Map<String,Integer> nation2Count=categoryNation2Count.get(category);
      if(nation2Count==null){
        nation2Count=new HashMap<String, Integer>();
        categoryNation2Count.put(category,nation2Count);
      }
      Integer count=nation2Count.get(tokens[1]);
      if(count==null){
        count=0;
        nation2Count.put(tokens[1],count);
      }
      nation2Count.put(tokens[1],count+1);
    }
    public void cleanup(Context context) throws IOException, InterruptedException {
      for(Map.Entry<Integer,Map<String,Integer>> entry:categoryNation2Count.entrySet()){
        for(Map.Entry<String,Integer> nationEntry: entry.getValue().entrySet()){
          context.write(new Text(entry.getKey()+"__"+nationEntry.getKey()),new IntWritable(nationEntry.getValue()));
        }
      }
    }

  }

  public static class CategoryAnalyzeReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
       int count=0;
       for(IntWritable value: values){
         count+=value.get();
       }
       context.write(key,new IntWritable(count));
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new CategoryAnalyzeTool(),args);
  }

}
