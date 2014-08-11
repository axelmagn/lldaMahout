package com.elex.bigdata.llda.mahout.mapreduce.est;

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
     FileSystem fs= FileSystem.get(conf);
     if(fs.exists(outputPath))
       fs.delete(outputPath,true);
     Job job=new Job(conf,"init topic term model");
     job.setMapperClass(InitTopicTermModelMapper.class);
     job.setReducerClass(VectorSumReducer.class);
     job.setMapOutputKeyClass(IntWritable.class);
     job.setMapOutputValueClass(VectorWritable.class);
     job.setInputFormatClass(SequenceFileInputFormat.class);
     FileInputFormat.addInputPath(job,inputPath);
     job.setOutputFormatClass(SequenceFileOutputFormat.class);
     SequenceFileOutputFormat.setOutputPath(job,outputPath);
     job.setJarByClass(InitTopicTermModelDriver.class);
     return job;
  }
  public static class InitTopicTermModelMapper extends Mapper<Text, MultiLabelVectorWritable,IntWritable,VectorWritable>{
    private int numTopics;
    private Matrix topicTermCount;
    private Random random;
    public void setup(Context context){
      Configuration conf = context.getConfiguration();
      numTopics = conf.getInt(LLDADriver.NUM_TOPICS, -1);
      int numTerms = conf.getInt(LLDADriver.NUM_TERMS, -1);
      topicTermCount=new SparseMatrix(numTopics,numTerms);
      long seed = conf.getLong(LLDADriver.RANDOM_SEED, 1234L);
      random= RandomUtils.getRandom(seed);
    }
    public void map(Text key,MultiLabelVectorWritable doc,Context context){
      Vector labels=new RandomAccessSparseVector(numTopics);
      for(int label: doc.getLabels())
        labels.setQuick(label,1.0);
      if(doc.getLabels().length==0){
        labels.assign(1.0);
      }
      train(labels,doc.getVector(),topicTermCount);
    }
    private void train(Vector labels,Vector doc,Matrix topicTermCountMatrix){
      Iterator<Vector.Element> labelIter=labels.iterateNonZero();
      while(labelIter.hasNext()){
        Vector.Element labelE=labelIter.next();
        Vector topicTermCountRow=topicTermCountMatrix.viewRow(labelE.index());
        Iterator<Vector.Element> docIter=labels.iterateNonZero();
        while(docIter.hasNext()){
          Vector.Element termE=docIter.next();
          if(topicTermCountRow.getQuick(termE.index())==0)
            topicTermCountRow.setQuick(termE.index(),random.nextDouble());
        }
      }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
      for(int i=0;i<topicTermCount.rowSize();i++){
        context.write(new IntWritable(i),new VectorWritable(topicTermCount.viewRow(i)));
      }
    }
  }

}
