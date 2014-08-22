package com.elex.bigdata.llda.mahout.mapreduce.inf;

import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocReducer;
import com.elex.bigdata.llda.mahout.mapreduce.est.LLDADriver;
import com.elex.bigdata.llda.mahout.model.LabeledModelTrainer;
import com.elex.bigdata.llda.mahout.model.LabeledTopicModel;
import com.elex.bigdata.llda.mahout.util.MathUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static org.apache.mahout.math.Vector.Element;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/22/14
 * Time: 5:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class LLDAInferenceMapper extends Mapper<Text, MultiLabelVectorWritable, Text, Text> {
  private static final Logger log = LoggerFactory.getLogger(LLDAInferenceMapper.class);
  private LabeledModelTrainer modelTrainer;
  private LabeledTopicModel readModel;
  private LabeledTopicModel writeModel;
  private int[] topics;

  protected LabeledModelTrainer getModelTrainer() {
    return modelTrainer;
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
    float modelWeight = conf.getFloat(LLDADriver.MODEL_WEIGHT, 1.0f);
    topics = LLDADriver.getTopics(conf);

    System.out.println("Initializing read model");
    Path[] modelPaths = LLDADriver.getModelPaths(conf);
    if (modelPaths != null && modelPaths.length > 0) {
      readModel = new LabeledTopicModel(conf, eta, alpha, null, numUpdateThreads, modelWeight, modelPaths);
      numTerms = readModel.getNumTerms();
    } else {
      log.info("No model files found");
      readModel = new LabeledTopicModel(topics, numTerms, eta, alpha, RandomUtils.getRandom(seed), null,
        numTrainThreads, modelWeight);
    }

    System.out.println("Initializing write model");
    writeModel = modelWeight == 1
      ? new LabeledTopicModel(topics, numTerms, eta, alpha, null, numUpdateThreads)
      : readModel;

    Map<Integer, Integer> child2ParentLabels = GenerateLDocReducer.getLabelRelations(conf);
    topics = new int[child2ParentLabels.size()];
    int i = 0;
    for (Integer topicLabel : child2ParentLabels.keySet())
      topics[i++] = topicLabel;

    System.out.println("Initializing model trainer");
    modelTrainer = new LabeledModelTrainer(readModel, writeModel, numTrainThreads, topics, numTerms);
    modelTrainer.start();
  }


  @Override
  public void map(Text uid, MultiLabelVectorWritable doc, Context context)
    throws IOException, InterruptedException {
    int[] labels;
    if (doc.getLabels().length > 0)
      labels = doc.getLabels();
    else
      labels = topics;
    LabeledModelTrainer modelTrainer = getModelTrainer();
    Vector result = modelTrainer.getReadModel().inf(doc.getVector(), labels);
    StringBuilder builder = new StringBuilder();
    Iterator<Element> iter = result.iterateNonZero();
    while (iter.hasNext()) {
      Element e = iter.next();
      builder.append(e.index() + ":" + e.get() + ",");
    }
    builder.deleteCharAt(builder.length() - 1);
    context.write(uid, new Text(builder.toString()));
  }

  @Override
  protected void cleanup(Context context) {
    getModelTrainer().stop();
  }
}
