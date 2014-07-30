package com.elex.bigdata.llda.mahout.mapreduce.analysis;

import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver;
import com.elex.bigdata.llda.mahout.data.inputformat.CombineTextInputFormat;
import com.elex.bigdata.llda.mahout.util.FileSystemUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/30/14
 * Time: 3:50 PM
 * To change this template use File | Settings | File Templates.
 */
public class GetWordHistoryDriver extends AbstractJob{
  public static final String UID_FILE="uid_file";
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption(GenerateLDocDriver.RESOURCE_ROOT,"rDir","specify the resources Dir",true);
    if(parseArguments(args)==null){
      return -1;
    }
    inputPath=getInputPath();
    outputPath=getOutputPath();
    Configuration conf=new Configuration();
    conf.set(UID_FILE,new Path(new Path(getOption(GenerateLDocDriver.RESOURCE_ROOT)),"specialUid").toString());
    Job job=prepareJob(conf,inputPath,outputPath);
    job.submit();
    job.waitForCompletion(true);
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }
  public Job prepareJob(Configuration conf,Path inputPath,Path outputPath) throws IOException {
    FileSystemUtil.setCombineInputSplitSize(conf,inputPath);
    FileSystem fs= FileSystem.get(conf);
    if(fs.exists(outputPath))
      fs.delete(outputPath,true);
    Job job=new Job(conf,"get word history");
    job.setMapperClass(GetWordHistoryMapper.class);
    job.setReducerClass(GetWordHistoryReducer.class);
    job.setInputFormatClass(CombineTextInputFormat.class);
    FileInputFormat.addInputPath(job,inputPath);
    job.setOutputFormatClass(TextOutputFormat.class);
    TextOutputFormat.setOutputPath(job,outputPath);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setJarByClass(GetWordHistoryDriver.class);
    return job;
  }
  public static class GetWordHistoryMapper extends Mapper<Object,Text,Text,Text> {
    private Set<String> uids=new HashSet<String>();
    public void setup(Context context) throws IOException {
      Configuration conf=context.getConfiguration();
      FileSystem fs= FileSystem.get(conf);
      Path uidPath=new Path(conf.get(UID_FILE));
      BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(uidPath)));
      String line;
      while((line=reader.readLine())!=null){
         uids.add(line.trim());
      }
    }
    public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
      String[] tokens=value.toString().split("\t");
      if(tokens.length<3)
        return;
      if(uids.contains(tokens[0])){
        context.write(new Text(tokens[0]),new Text(tokens[1]));
      }
    }
  }
  public static class GetWordHistoryReducer extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key,Iterable<Text> value,Context context) throws IOException, InterruptedException {
      StringBuilder builder=new StringBuilder();
      for(Text word:value){
         builder.append(word.toString()+"\t");
      }
      context.write(key,new Text(builder.toString()));
    }
  }
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new GetWordHistoryDriver(),args);
  }
}
