package com.elex.bigdata.llda.mahout.mapreduce.analysis.category;

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
  /**
     tool to analyze user num for each category-nation
     这个类用来分析各个类别中各个国家的用户数
     在将推理的结果etl到本地文件（0.0）中后，对其进行分析，得到分析结果（即各个类别各有多少用户）。
     将这两个文件copy到hdfs中的CATEGORY_RESULT_DIR中，文件名为userCategory和analytics。
     根据这两个文件和早已计算出的用户国家（或语言）数据得到各个类别中各个国家的用户数量。
   */
  public static final String CATEGORY_RESULT_DIR="category_result_dir";
  public static final String CATEGORY_FILE="userCategory";
  public static final String ANALYTIC_FILE="analytics";

  @Override
  public int run(String[] args) throws Exception {
    addOption(CATEGORY_RESULT_DIR,"crd","specify category result directory ",true);
    //用户——国家数据
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
    //map from category to bloomFilter
    //bloomFilter 中存储了该类别所对应的用户的cookieId
    private Map<Integer,BloomFilter> category2BloomFilter=new HashMap<Integer,BloomFilter>();
    //存储结果的map
    private Map<Integer,Map<String,Integer>> categoryNation2Count=new HashMap<Integer, Map<String, Integer>>();
    private int sampleNum=0;
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
        if(tokens.length<2)
          continue;
        System.out.println(Integer.parseInt(tokens[0])+"\t"+Integer.parseInt(tokens[1]));
        category2BloomFilter.put(Integer.parseInt(tokens[0]), new BloomFilter(3*Integer.parseInt(tokens[1]), 3, 0));
      }
      reader.close();
      reader=new BufferedReader(new InputStreamReader(fs.open(categoryFile)));
      int num=0;
      while((line=reader.readLine())!=null){
        String[] tokens=line.split("\t");
        if(tokens.length<2)
          continue;
        num++;
        Key key=new Key(Bytes.toBytes(tokens[0]));
        if(key.getBytes().length<1)
        {
          System.out.println(key);
          continue;
        }
        if(num%20000==1)
          System.out.println("category "+Integer.parseInt(tokens[1]));
        category2BloomFilter.get(Integer.parseInt(tokens[1])).add(key);
      }
    }
    public void map(Object key,Text value,Context context){
      String[] tokens=value.toString().trim().split("\t");
      if(tokens.length<2)
        return;
      Integer category=null;
      sampleNum++;
      //找到该用户的类别
      for(Map.Entry<Integer,BloomFilter> entry:category2BloomFilter.entrySet()){
        if(entry.getValue().membershipTest(new Key(Bytes.toBytes(tokens[0])))){
          category=entry.getKey();
          break;
        }
      }
      if(category==null)
      {
        System.out.println(" not hit "+tokens[0]);
        return;
      }
      //将该类别中该用户所对应的国家中用户数量加1
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
