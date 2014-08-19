package com.elex.bigdata.llda.mahout.mapreduce.est;

import com.elex.bigdata.llda.mahout.util.FileSystemUtil;
import com.elex.bigdata.llda.mahout.util.MathUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.common.mapreduce.VectorSumReducer;
import org.apache.mahout.math.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/11/14
 * Time: 3:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class InitTopicTermModelDriver extends AbstractJob{
  @Override
  public int run(String[] args) throws Exception {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }
  public static void runJob(Configuration conf,Path inputPath,Path outputPath) throws IOException, ClassNotFoundException, InterruptedException {
     InitTopicTermModelDriver initTopicTermModelDriver=new InitTopicTermModelDriver();
     Job job=initTopicTermModelDriver.prepareJob(conf,inputPath,outputPath);
     job.submit();
     job.waitForCompletion(true);
  }
  public Job prepareJob(Configuration conf,Path inputPath,Path outputPath) throws IOException {
     FileSystemUtil.deleteOutputPath(conf,outputPath);
     Job job=new Job(conf,"init topic term model");
     job.setMapperClass(InitTopicTermModelMapper.class);
     job.setCombinerClass(VectorSumReducer.class);
     job.setReducerClass(VectorSumReducer.class);
     job.setMapOutputKeyClass(IntWritable.class);
     job.setMapOutputValueClass(VectorWritable.class);
     job.setOutputKeyClass(IntWritable.class);
     job.setOutputValueClass(VectorWritable.class);
     job.setInputFormatClass(SequenceFileInputFormat.class);
     FileInputFormat.addInputPath(job,inputPath);
     job.setOutputFormatClass(SequenceFileOutputFormat.class);
     SequenceFileOutputFormat.setOutputPath(job,outputPath);
     job.setJarByClass(InitTopicTermModelDriver.class);
     return job;
  }
  public static class InitTopicTermModelMapper extends Mapper<Text, MultiLabelVectorWritable,IntWritable,VectorWritable>{
    private int[] topics;
    private int numTerms;
    private Random random;
    private int num=0;
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      topics=LLDADriver.getTopics(conf);
      numTerms = conf.getInt(LLDADriver.NUM_TERMS, -1);
      System.out.println("numTerms "+numTerms);
      long seed = conf.getLong(LLDADriver.RANDOM_SEED, 1234L);
      random= RandomUtils.getRandom(seed);
    }

    public void map(Text key,MultiLabelVectorWritable lDoc,Context context) throws IOException, InterruptedException {
      int[] labels=lDoc.getLabels();
      if(labels.length==0)
        labels=topics;
      num++;
      Vector doc=lDoc.getVector();
      boolean shouldLog=false;
      if(num%50000==1)
      {
        shouldLog=true;
        System.out.println("num "+num);
      }
      for(int label: labels){
        Vector topicTermCountRow=new RandomAccessSparseVector(numTerms);
        Iterator<Vector.Element> docIter=doc.iterateNonZero();
        if(shouldLog)
          System.out.println("topic "+label);
        while(docIter.hasNext()){
          Vector.Element termE=docIter.next();
          double count=Math.abs(random.nextDouble());
          topicTermCountRow.setQuick(termE.index(),count);
          context.write(new IntWritable(label),new VectorWritable(topicTermCountRow));
          if(shouldLog){
            System.out.print(" term:"+termE.index()+" count:"+count+" , ");
          }
        }
        if(shouldLog)
          System.out.println();
      }

    }

  }

}
