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
    private Matrix topicTermCount;
    private Random random;
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      topics=LLDADriver.getTopics(conf);
      int numTerms = conf.getInt(LLDADriver.NUM_TERMS, -1);
      topicTermCount=new SparseMatrix(MathUtil.getMax(topics)+1,numTerms);
      long seed = conf.getLong(LLDADriver.RANDOM_SEED, 1234L);
      random= RandomUtils.getRandom(seed);
    }

    public void map(Text key,MultiLabelVectorWritable doc,Context context){
      int[] labels=doc.getLabels();
      if(labels.length==0)
        labels=topics;
      train(doc.getVector(),labels,topicTermCount);
    }
    private void train(Vector doc,int[] labels,Matrix topicTermCountMatrix){
      for(int label: labels){
        Vector topicTermCountRow=topicTermCountMatrix.viewRow(label);
        Iterator<Vector.Element> docIter=doc.iterateNonZero();
        while(docIter.hasNext()){
          Vector.Element termE=docIter.next();
          topicTermCountRow.setQuick(termE.index(),Math.abs(random.nextDouble()));
        }
      }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
      for(int topic: topics){
        context.write(new IntWritable(topic),new VectorWritable(topicTermCount.viewRow(topic)));
      }
    }
  }

}
