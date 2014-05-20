package com.elex.bigdata.llda.mahout.mapreduce;

import com.elex.bigdata.llda.mahout.data.LabeledDocumentWritable;
import com.elex.bigdata.llda.mahout.model.LabeledModelTrainer;
import com.elex.bigdata.llda.mahout.model.LabeledTopicModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.lda.cvb.CVB0Driver;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/22/14
 * Time: 5:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class LLDAInferenceMapper extends Mapper<Text, LabeledDocumentWritable, Text, Text> {
  private static final Logger log = LoggerFactory.getLogger(LLDAInferenceMapper.class);
  private LabeledModelTrainer modelTrainer;
  private LabeledTopicModel readModel;
  private LabeledTopicModel writeModel;
  private int maxIters;
  private int numTopics;

  private final VectorWritable topics = new VectorWritable();

  protected LabeledModelTrainer getModelTrainer() {
    return modelTrainer;
  }

  protected int getMaxIters() {
    return maxIters;
  }

  protected int getNumTopics() {
    return numTopics;
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    log.info("Retrieving configuration");
    Configuration conf = context.getConfiguration();
    float eta = conf.getFloat(CVB0Driver.TERM_TOPIC_SMOOTHING, Float.NaN);
    float alpha = conf.getFloat(CVB0Driver.DOC_TOPIC_SMOOTHING, Float.NaN);
    long seed = conf.getLong(CVB0Driver.RANDOM_SEED, 1234L);
    numTopics = conf.getInt(CVB0Driver.NUM_TOPICS, -1);
    int numTerms = conf.getInt(CVB0Driver.NUM_TERMS, -1);
    int numUpdateThreads = conf.getInt(CVB0Driver.NUM_UPDATE_THREADS, 1);
    int numTrainThreads = conf.getInt(CVB0Driver.NUM_TRAIN_THREADS, 4);
    maxIters = conf.getInt(CVB0Driver.MAX_ITERATIONS_PER_DOC, 10);
    float modelWeight = conf.getFloat(CVB0Driver.MODEL_WEIGHT, 1.0f);

    log.info("Initializing read model");
    Path[] modelPaths = CVB0Driver.getModelPaths(conf);
    if (modelPaths != null && modelPaths.length > 0) {
      readModel = new LabeledTopicModel(conf, eta, alpha, null, numUpdateThreads, modelWeight, modelPaths);
    } else {
      log.info("No model files found");
      readModel = new LabeledTopicModel(numTopics, numTerms, eta, alpha, RandomUtils.getRandom(seed), null,
        numTrainThreads, modelWeight);
    }

    log.info("Initializing write model");
    writeModel = modelWeight == 1
      ? new LabeledTopicModel(numTopics, numTerms, eta, alpha, null, numUpdateThreads)
      : readModel;

    log.info("Initializing model trainer");
    modelTrainer = new LabeledModelTrainer(readModel, writeModel, numTrainThreads, numTopics, numTerms);
    modelTrainer.start();
  }


  @Override
  public void map(Text uid, LabeledDocumentWritable doc, Context context)
    throws IOException, InterruptedException {
    /*
    int numTopics = getNumTopics();
    int[] labels = doc.getLabels();
    Vector topicVector = new DenseVector(numTopics);
    if (labels.length > 0) {
      topicVector.assign(0);
      for (int i = 0; i < labels.length; i++) {
        topicVector.set(labels[i], (double) 1l / labels.length);
      }
    } else {
      topicVector.assign(1l/topicVector.size());
    }
    Matrix docModel = new SparseRowMatrix(numTopics, doc.getVector().size());
    int maxIters = getMaxIters();
    ModelTrainer modelTrainer = getModelTrainer();
    for (int i = 0; i < maxIters; i++) {
      modelTrainer.getReadModel().trainDocTopicModel(doc.getVector(), topicVector, docModel);
    }
    topics.set(topicVector);
    context.write(uid, topics);
    */
    int numTopics=getNumTopics();
    Vector labels=doc.get().getLabels();
    Matrix docModel = new SparseRowMatrix(numTopics,doc.get().getUrlCounts().size());
    int maxIters=getMaxIters();
    LabeledModelTrainer modelTrainer=getModelTrainer();
    for(int i=0;i<maxIters;i++){
      modelTrainer.getReadModel().trainDocTopicModel(doc.get().getUrlCounts(),labels,docModel);
    }
    StringBuilder builder=new StringBuilder();
    for(Vector.Element e: labels){
       builder.append(e.index()+":"+e.get()+"\t");
    }
    context.write(uid,new Text(builder.toString()));
  }

  @Override
  protected void cleanup(Context context) {
    getModelTrainer().stop();
  }
}
