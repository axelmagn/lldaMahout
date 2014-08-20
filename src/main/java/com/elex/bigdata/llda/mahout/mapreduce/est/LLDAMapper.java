package com.elex.bigdata.llda.mahout.mapreduce.est;

import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocReducer;
import com.elex.bigdata.llda.mahout.model.LabeledModelTrainer;
import com.elex.bigdata.llda.mahout.model.LabeledTopicModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/22/14
 * Time: 4:58 PM
 * To change this template use File | Settings | File Templates.
 */
public class LLDAMapper extends Mapper<Text, MultiLabelVectorWritable, IntWritable, VectorWritable> {
  private static final Logger log = LoggerFactory.getLogger(LLDAMapper.class);

  private LabeledModelTrainer modelTrainer;
  private LabeledTopicModel readModel;
  private LabeledTopicModel writeModel;
  private int maxIters;
  private int sampleRatio=50000,index=0;
  private int[] topics;

  protected LabeledModelTrainer getModelTrainer() {
    return modelTrainer;
  }

  protected int getMaxIters() {
    return maxIters;
  }

  protected int[] getTopics() {
    return topics;
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    log.info("Retrieving configuration");
    Configuration conf = context.getConfiguration();
    float eta = conf.getFloat(LLDADriver.TERM_TOPIC_SMOOTHING, Float.NaN);
    float alpha = conf.getFloat(LLDADriver.DOC_TOPIC_SMOOTHING, Float.NaN);
    long seed = conf.getLong(LLDADriver.RANDOM_SEED, 1234L);
    int numTerms = conf.getInt(LLDADriver.NUM_TERMS, -1);
    int numUpdateThreads = conf.getInt(LLDADriver.NUM_UPDATE_THREADS, 1);
    int numTrainThreads = conf.getInt(LLDADriver.NUM_TRAIN_THREADS, 4);
    maxIters = conf.getInt(LLDADriver.MAX_ITERATIONS_PER_DOC, 10);
    float modelWeight = conf.getFloat(LLDADriver.MODEL_WEIGHT, 1.0f);
    topics=LLDADriver.getTopics(conf);
    log.info("Initializing read model");
    Path[] modelPaths = LLDADriver.getModelPaths(conf);
    if (modelPaths != null && modelPaths.length > 0) {
      readModel = new LabeledTopicModel(conf, eta, alpha, null, numUpdateThreads, modelWeight, modelPaths);
    } else {
      log.info("No model files found");
      readModel = new LabeledTopicModel(topics, numTerms, eta, alpha, RandomUtils.getRandom(seed), null,
        numTrainThreads, modelWeight);
    }

    log.info("Initializing write model");
    writeModel = modelWeight == 1
      ? new LabeledTopicModel(topics, numTerms, eta, alpha, null, numUpdateThreads)
      : readModel;

    log.info("Initializing model trainer");
    modelTrainer = new LabeledModelTrainer(readModel, writeModel, numTrainThreads, topics, numTerms);
    modelTrainer.start();
  }

  @Override
  public void map(Text uid, MultiLabelVectorWritable doc, Context context)
    throws IOException, InterruptedException {
    int[] labels=doc.getLabels();
    if(labels.length==0)
      labels=topics;
    if((index++)>=sampleRatio){
      index=0;
      StringBuilder vectorStr=new StringBuilder();
      Iterator<Vector.Element> docIter=doc.getVector().iterateNonZero();
      while(docIter.hasNext()){
        Vector.Element e=docIter.next();
        vectorStr.append(e.index()+":"+e.get()+"  ");
      }
      log.info("vector is : "+vectorStr.toString());
      StringBuilder labelStr=new StringBuilder();
      for(int label: labels)
        labelStr.append(label+" ");
      log.info("labels is: "+labelStr.toString());
    }
    modelTrainer.train(doc.getVector(), labels, true);
  }

  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    log.info("Stopping model trainer");
    modelTrainer.stop();

    log.info("Writing model");
    LabeledTopicModel readFrom = modelTrainer.getReadModel();
    for (MatrixSlice topic : readFrom) {
      System.out.println("topic:"+topic.index()+","+"sum:"+topic.vector().norm(1.0));
      context.write(new IntWritable(topic.index()), new VectorWritable(topic.vector()));
    }
    readModel.stop();
    writeModel.stop();
  }
}
