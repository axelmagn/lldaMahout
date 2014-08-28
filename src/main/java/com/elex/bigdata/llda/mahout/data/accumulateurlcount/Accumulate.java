package com.elex.bigdata.llda.mahout.data.accumulateurlcount;

import com.elex.bigdata.llda.mahout.data.accumulateurlcount.tables.*;
import com.elex.bigdata.llda.mahout.util.FileSystemUtil;
import com.elex.bigdata.util.MetricMapping;
import com.xingcloud.xa.hbase.filter.SkipScanFilter;
import com.xingcloud.xa.hbase.model.KeyRange;
import com.xingcloud.xa.hbase.model.KeyRangeComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
  public static String TABLE_TYPE="table_type";

  private static final String[] CustomTables = new String[]{"dmp_user_action", "yac_user_action"};
  private static final String TABLE_NAV="nav_all",TABLE_AD="ad_all_log",TABLE_GM337="gm_user_action";
  public Accumulate() {

  }

  public Accumulate(String outputBase, String startTime, String endTime) throws ParseException, IOException {
    init(outputBase, startTime, endTime);
  }

  private void init(String outputBase, String startTime, String endTime) throws ParseException, IOException {
    this.outputBase = outputBase;
    output=outputBase+File.separator+startTime+"_"+endTime;
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    startTimeStamp = dateFormat.parse(startTime).getTime();
    endTimeStamp = dateFormat.parse(endTime).getTime();
    conf = HBaseConfiguration.create();
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
    runJob(TABLE_AD,new AdTable(),startTimeStamp,endTimeStamp);
    runJob(TABLE_NAV,new NavTable(),startTimeStamp,endTimeStamp);
    runJob(TABLE_GM337,new Gm337Table(),startTimeStamp,endTimeStamp);
    for(String tableName: CustomTables){
      runJob(tableName,new CustomTable(),startTimeStamp,endTimeStamp);
    }
    return 0;
  }

  public void runJob(String tableName,SuperTable tabletype,long startTime,long endTime) throws IOException, ClassNotFoundException, InterruptedException {
    Path outputPath=new Path(output,tableName);
    FileSystemUtil.deleteOutputPath(conf,outputPath);
    conf.set(TABLE_TYPE,tabletype.getClass().getName());
    Job job=new Job(conf,"accumulate "+tableName+" "+outputPath.getName());
    Scan scan=tabletype.getScan(startTime,endTime);
    TableMapReduceUtil.initTableMapperJob(tableName,scan,AccumulateMapper.class,Text.class, IntWritable.class,job);
    job.setReducerClass(AccumulateReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setJarByClass(Accumulate.class);
    job.submit();
    job.waitForCompletion(true);
  }
}