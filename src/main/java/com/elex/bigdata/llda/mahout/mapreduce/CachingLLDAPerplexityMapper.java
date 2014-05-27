package com.elex.bigdata.llda.mahout.mapreduce;

import com.elex.bigdata.llda.mahout.model.LabeledModelTrainer;
import com.elex.bigdata.llda.mahout.model.LabeledTopicModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.clustering.lda.cvb.CVB0Driver;
import org.apache.mahout.clustering.lda.cvb.ModelTrainer;
import org.apache.mahout.clustering.lda.cvb.TopicModel;
import org.apache.mahout.common.MemoryUtil;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.MultiLabelVectorWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/27/14
 * Time: 3:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class CachingLLDAPerplexityMapper  extends
    Mapper<Text, MultiLabelVectorWritable, DoubleWritable, DoubleWritable> {
    /**
     * Hadoop counters for {@link CachingLLDAPerplexityMapper}, to aid in debugging.
     */
    public enum Counters {
      SAMPLED_DOCUMENTS
    }

    private static final Logger log = LoggerFactory.getLogger(CachingLLDAPerplexityMapper.class);

    private LabeledModelTrainer modelTrainer;
    private int maxIters;
    private int numTopics;
    private float testFraction;
    private Random random;
    private Vector topicVector;
    private final DoubleWritable outKey = new DoubleWritable();
    private final DoubleWritable outValue = new DoubleWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      MemoryUtil.startMemoryLogger(5000);

      log.info("Retrieving configuration");
      Configuration conf = context.getConfiguration();
      float eta = conf.getFloat(CVB0Driver.TERM_TOPIC_SMOOTHING, Float.NaN);
      float alpha = conf.getFloat(CVB0Driver.DOC_TOPIC_SMOOTHING, Float.NaN);
      long seed = conf.getLong(CVB0Driver.RANDOM_SEED, 1234L);
      random = RandomUtils.getRandom(seed);
      numTopics = conf.getInt(CVB0Driver.NUM_TOPICS, -1);
      int numTerms = conf.getInt(CVB0Driver.NUM_TERMS, -1);
      int numUpdateThreads = conf.getInt(CVB0Driver.NUM_UPDATE_THREADS, 1);
      int numTrainThreads = conf.getInt(CVB0Driver.NUM_TRAIN_THREADS, 4);
      maxIters = conf.getInt(CVB0Driver.MAX_ITERATIONS_PER_DOC, 10);
      float modelWeight = conf.getFloat(CVB0Driver.MODEL_WEIGHT, 1.0f);
      testFraction = conf.getFloat(CVB0Driver.TEST_SET_FRACTION, 0.1f);

      log.info("Initializing read model");
      LabeledTopicModel readModel;
      Path[] modelPaths = CVB0Driver.getModelPaths(conf);
      if (modelPaths != null && modelPaths.length > 0) {
        readModel = new LabeledTopicModel(conf, eta, alpha, null, numUpdateThreads, modelWeight, modelPaths);
      } else {
        log.info("No model files found");
        readModel = new LabeledTopicModel(numTopics, numTerms, eta, alpha, RandomUtils.getRandom(seed), null,
          numTrainThreads, modelWeight);
      }

      log.info("Initializing model trainer");
      modelTrainer = new LabeledModelTrainer(readModel, null, numTrainThreads, numTopics, numTerms);

      log.info("Initializing topic vector");
      topicVector = new DenseVector(new double[numTopics]);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      MemoryUtil.stopMemoryLogger();
    }

    @Override
    public void map(Text uid, MultiLabelVectorWritable document, Context context)
      throws IOException, InterruptedException{
      if (1 > testFraction && random.nextFloat() >= testFraction) {
        return;
      }
      context.getCounter(Counters.SAMPLED_DOCUMENTS).increment(1);
      outKey.set(document.getVector().norm(1));
      outValue.set(modelTrainer.calculatePerplexity(document.getVector(), topicVector.assign(1.0 / numTopics), maxIters));
      context.write(outKey, outValue);
    }
  }
