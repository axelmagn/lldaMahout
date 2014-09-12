package com.elex.bigdata.llda.mahout.mapreduce.analysis.word;

import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver;
import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocReducer;
import com.elex.bigdata.llda.mahout.data.inputformat.CombineTextInputFormat;
import com.elex.bigdata.llda.mahout.dictionary.UpdateDictReducer;
import com.elex.bigdata.llda.mahout.util.FileSystemUtil;
import com.elex.bigdata.llda.mahout.util.PrefixTrie;
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
import org.apache.mahout.common.Pair;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/10/14
 * Time: 3:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class IdfDriver extends AbstractJob {
  public static int DEFAULT_MIN_COUNT=5;
  public static final String CALSSIFIED_URL="classified_url";

  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption(GenerateLDocDriver.RESOURCE_ROOT,"rDir","specify the resources Dir",true);
    addOption(CALSSIFIED_URL, "fcu", "specify if filter url classified", true);
    if(parseArguments(args)==null)
      return -1;
    Configuration conf=getConf();
    String resourceRoot=getOption(GenerateLDocDriver.RESOURCE_ROOT);
    conf.set(GenerateLDocDriver.RESOURCE_ROOT,resourceRoot);
    String actionStr=getOption(CALSSIFIED_URL);
    System.out.println(resourceRoot+"\t"+actionStr);
    conf.set(CALSSIFIED_URL,actionStr);
    Job job=prepareJob(conf,getInputPath(),getOutputPath());
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
    private ACTION action;
    private PrefixTrie prefixTrie=new PrefixTrie();
    private Set<String> urls=new HashSet<String>();
    public void setup(Context context) throws IOException {
       Configuration conf=context.getConfiguration();
       String actionStr=conf.get(CALSSIFIED_URL);
       System.out.println(actionStr);
       if(!ACTION.valueStrs().contains(actionStr)){
         System.out.println("for option classified_url,you should input filter,get or pass");
         throw new IOException("for option classified_url,you should input filter,get or pass");
       }
       action=ACTION.valueOf(actionStr);
       if(action!=ACTION.PASS){
         Pair<Map<String,String>,Map<String,Integer>> pair=GenerateLDocReducer.loadUrlTopics(conf,prefixTrie);
         urls=pair.getFirst().keySet();
       }
    }
    public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
      String[] tokens=value.toString().split("\t");
      if(tokens.length<3)
        return;
      switch (action){
        case FILTER:
          if(prefixTrie.prefixSearch(tokens[1])!=-1||urls.contains(tokens[1]))
            context.write(new Text(tokens[1]),new Text(tokens[0]+"\t"+tokens[2]));
          break;
        case GET:
          if(prefixTrie.prefixSearch(tokens[1])==-1 && !urls.contains(tokens[1]))
            context.write(new Text(tokens[1]),new Text(tokens[0]+"\t"+tokens[2]));
          break;
        default:
          context.write(new Text(tokens[1]),new Text(tokens[0]+"\t"+tokens[2]));
      }
    }
  }

  public static class IdfReducer extends Reducer<Text,Text,Text,DoubleWritable> {
    private double numSum=30*10000;
    double  LOG2=Math.log(2);
    public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
      Set<String> uids=new HashSet<String>();
      int count=0;
      for(Text value: values){
        String[] tokens=value.toString().split("\t");
        uids.add(tokens[0]);
        count+=Integer.parseInt(tokens[1]);
      }
      if (count>DEFAULT_MIN_COUNT && uids.size()>=5)
        context.write(new Text(key.toString()+"\t"+uids.size()+"\t"+count),new DoubleWritable(Math.log(numSum/uids.size())/LOG2));
    }
  }

  public  enum ACTION{
    FILTER("filter"),
    GET("get"),
    PASS("pass");
    String action;
    ACTION(String action){
      this.action=action;
    }
    public static Set<String> valueStrs(){
      Set<String> strs=new HashSet<String>();
      strs.add("filter");strs.add("get");strs.add("pass");
      return strs;
    }

    public String toString(){
      return action;
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new IdfDriver(),args);
  }
}
