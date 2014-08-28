package com.elex.bigdata.llda.mahout.data.accumulateurlcount;

import com.elex.bigdata.llda.mahout.data.accumulateurlcount.tables.*;
import com.elex.bigdata.llda.mahout.util.FileSystemUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/11/14
 * Time: 5:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class Accumulate extends AbstractJob {
  String outputBase,output;
  private long startTimeStamp, endTimeStamp;

  private Configuration conf;
  // Option string
  private static String OUTPUT_BASE = "outputBase";
  private static String STARTTIME = "startTime";
  private static String ENDTIME = "endTime";
  public static String TABLE_TYPE= "table_type";

  private Map<String,SuperTable> table2Type=new HashMap<String, SuperTable>();
  public Accumulate() {

  }

  public Accumulate(String outputBase, String startTime, String endTime) throws ParseException, IOException, IllegalAccessException, InstantiationException, ClassNotFoundException {
    init(outputBase, startTime, endTime);
  }

  private void init(String outputBase, String startTime, String endTime) throws ParseException, IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
    this.outputBase = outputBase;
    output=outputBase+File.separator+startTime+"_"+endTime;
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    startTimeStamp = dateFormat.parse(startTime).getTime();
    endTimeStamp = dateFormat.parse(endTime).getTime();
    conf = HBaseConfiguration.create();
    Properties properties=new Properties();
    properties.load(this.getClass().getResourceAsStream("/table_type"));
    for(String key: properties.stringPropertyNames()){
       SuperTable superTable=(SuperTable)Class.forName(properties.getProperty(key)).newInstance();
       table2Type.put(key,superTable);
       System.out.println("table: "+key+" type:"+superTable.getClass().getName());
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(HBaseConfiguration.create(), new Accumulate(), args);

  }

  @Override
  public int run(String[] args) throws Exception {
    addOption(OUTPUT_BASE, "ob", "output base dir in hdfs");
    addOption(STARTTIME, "st", "start time of url access");
    addOption(ENDTIME, "et", "end time of url access");
    if (parseArguments(args) == null)
      return -1;
    init(getOption(OUTPUT_BASE), getOption(STARTTIME), getOption(ENDTIME));
    JobControl jobControl=new JobControl("accumulate "+startTimeStamp+" "+endTimeStamp);
    ControlledJob preJob=null;
    for(Map.Entry<String,SuperTable> entry: table2Type.entrySet()){
      Job job=prepareJob(entry.getKey(),entry.getValue(),startTimeStamp,endTimeStamp);
      ControlledJob currentJob=new ControlledJob(conf);
      currentJob.setJob(job);
      if(preJob!=null)
        currentJob.addDependingJob(preJob);
      preJob=currentJob;
      jobControl.addJob(preJob);
    }
    jobControl.run();
    Thread jcThread=new Thread(jobControl);
    jcThread.start();
    while(true){
      if(jobControl.allFinished()){
        System.out.println(jobControl.getSuccessfulJobList());
        jobControl.stop();
        return 0;
      }
      if(jobControl.getFailedJobList().size()>0){
        System.out.println(jobControl.getFailedJobList());
        jobControl.stop();
        return 1;
      }
    }
  }

  public Job prepareJob(String tableName,SuperTable tabletype,long startTime,long endTime) throws IOException, ClassNotFoundException, InterruptedException {
    Path outputPath=new Path(output,tableName);
    FileSystemUtil.deleteOutputPath(conf,outputPath);
    conf.set(TABLE_TYPE,tabletype.getClass().getName());
    Job job=new Job(conf,"accumulate "+tableName+" "+outputPath.toString());
    Scan scan=tabletype.getScan(startTime,endTime);
    TableMapReduceUtil.initTableMapperJob(tableName, scan, AccumulateMapper.class, Text.class, IntWritable.class, job);
    //job.setReducerClass(AccumulateReducer.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    //job.setNumReduceTasks(4);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setJarByClass(Accumulate.class);
    return job;
  }
}