package com.elex.bigdata.llda.mahout.model;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.clustering.lda.cvb.TopicModel;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.*;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;
import org.apache.mahout.math.stats.Sampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/15/14
 * Time: 3:42 PM
 * To change this template use File | Settings | File Templates.
 */
public class LabeledTopicModel implements Configurable, Iterable<MatrixSlice> {

  private static final Logger log = LoggerFactory.getLogger(LabeledTopicModel.class);

  private final String[] dictionary;
  private final AbstractMatrix topicTermCounts;
  private final Vector topicSums;
  private final int[] topics;
  private final int numTerms;
  private final double eta;
  private final double alpha;

  private Configuration conf;

  private final Sampler sampler;
  private final int numThreads;
  private ThreadPoolExecutor threadPool;
  private Updater[] updaters;

  public int getNumTerms() {
    return numTerms;
  }

  public int[] getTopics() {
    return topics;
  }

  public LabeledTopicModel(int[] topics, int numTerms, double eta, double alpha, String[] dictionary,
                           double modelWeight) {
    this(topics, numTerms, eta, alpha, null, dictionary, 1, modelWeight);
  }

  public LabeledTopicModel(Configuration conf, double eta, double alpha,
                           String[] dictionary, int numThreads, double modelWeight, Path... modelpath) throws IOException {
    this(loadModel(conf, modelpath), eta, alpha, dictionary, numThreads, modelWeight);
  }

  public LabeledTopicModel(int[] topics, int numTerms, double eta, double alpha, Random random,
                           String[] dictionary, int numThreads, double modelWeight) {
    this(randomMatrix(topics, numTerms, random), eta, alpha, dictionary, numThreads, modelWeight);
  }

  private LabeledTopicModel(Pair<AbstractMatrix, Vector> model, double eta, double alpha, String[] dict,
                            int numThreads, double modelWeight) {
    this(model.getFirst(), model.getSecond(), eta, alpha, dict, numThreads, modelWeight);
  }

  public LabeledTopicModel(AbstractMatrix topicTermCounts, Vector topicSums,  double eta, double alpha,
                           String[] dictionary, int numThreads, double modelWeight) {
    this.dictionary = dictionary;
    this.topicTermCounts = topicTermCounts;
    this.topicSums = topicSums;
    this.topics=new int[topicSums.size()];
    Iterator<Vector.Element> iter=topicSums.iterateNonZero();
    for(int i=0;i<topics.length;i++)
      topics[i]=iter.next().index();
    this.numTerms = topicTermCounts.numCols();
    this.eta = eta;
    this.alpha = alpha;
    this.sampler = new Sampler(RandomUtils.getRandom());
    this.numThreads = numThreads;
    if (modelWeight != 1) {
      topicSums.assign(Functions.mult(modelWeight));
      for (int topic: topics) {
        topicTermCounts.viewRow(topic).assign(Functions.mult(modelWeight));
      }
    }
    initializeThreadPool();
  }

  private static Vector viewRowSums(Matrix m) {
    Vector v = new RandomAccessSparseVector(m.numRows());
    for (MatrixSlice slice : m) {
      v.setQuick(slice.index(), slice.vector().norm(1));
    }
    return v;
  }

  private synchronized void initializeThreadPool() {
    if (threadPool != null) {
      threadPool.shutdown();
      try {
        threadPool.awaitTermination(100, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        log.error("Could not terminate all threads for TopicModel in time.", e);
      }
    }
    threadPool = new ThreadPoolExecutor(numThreads, numThreads, 0, TimeUnit.SECONDS,
      new ArrayBlockingQueue<Runnable>(numThreads * 10));
    threadPool.allowCoreThreadTimeOut(false);
    updaters = new Updater[numThreads];
    for (int i = 0; i < numThreads; i++) {
      updaters[i] = new Updater();
      threadPool.submit(updaters[i]);
    }
  }

  Matrix topicTermCounts() {
    return topicTermCounts;
  }

  @Override
  public Iterator<MatrixSlice> iterator() {
    return topicTermCounts.iterator();
  }

  public Vector topicSums() {
    return topicSums;
  }

  private static Pair<AbstractMatrix, Vector> randomMatrix(int[] topics, int numTerms, Random random) {
    AbstractMatrix topicTermCounts = new SparseMatrix(topics.length, numTerms);
    Vector topicSums = new RandomAccessSparseVector(topics.length);
    if (random != null) {
      for (int x = 0; x < topics.length; x++) {
        for (int term = 0; term < numTerms; term++) {
          topicTermCounts.viewRow(x).set(term, random.nextDouble());
        }
      }
    }
    for (int topic: topics) {
      topicSums.setQuick(topic, random == null ? 1.0 : topicTermCounts.viewRow(topic).norm(1));
    }
    return Pair.of(topicTermCounts, topicSums);
  }

  public static Pair<AbstractMatrix, Vector> loadModel(Configuration conf, Path... modelPaths)
    throws IOException {
    int numTopics = 0;
    int numTerms = -1;
    List<Pair<Integer, Vector>> rows = Lists.newArrayList();
    for (Path modelPath : modelPaths) {
      log.info("load model from {}", modelPath.toString());
      for (Pair<IntWritable, VectorWritable> row
        : new SequenceFileIterable<IntWritable, VectorWritable>(modelPath, true, conf)) {
        rows.add(Pair.of(row.getFirst().get(), row.getSecond().get()));
        numTopics++;
        if (numTerms < 0) {
          numTerms = row.getSecond().get().size();
        }
      }
    }
    if (rows.isEmpty()) {
      throw new IOException(java.util.Arrays.toString(modelPaths) + " have no vectors in it");
    }
    log.info("numTopics is {},numTerms is {}", numTopics, numTerms);
    AbstractMatrix model = new SparseMatrix(numTopics, numTerms);
    Vector topicSums = new RandomAccessSparseVector(numTopics);
    for (Pair<Integer, Vector> pair : rows) {
      model.viewRow(pair.getFirst()).assign(pair.getSecond());
      topicSums.setQuick(pair.getFirst(), pair.getSecond().norm(1));
    }
    return Pair.of(model, topicSums);
  }

  // NOTE: this is purely for debug purposes.  It is not performant to "toString()" a real model
  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    for (int topic: topics) {
      String v = dictionary != null
        ? vectorToSortedString(topicTermCounts.viewRow(topic).normalize(1), dictionary)
        : topicTermCounts.viewRow(topic).asFormatString();
      buf.append(v).append('\n');
    }
    return buf.toString();
  }

  public int sampleTerm(Vector topicDistribution) {
    return sampler.sample(topicTermCounts.viewRow(sampler.sample(topicDistribution)));
  }

  public int sampleTerm(int topic) {
    return sampler.sample(topicTermCounts.viewRow(topic));
  }

  public synchronized void reset() {
    for (int topic: topics ) {
      topicTermCounts.assignRow(topic, new SequentialAccessSparseVector(numTerms));
    }
    topicSums.assign(1.0);
    if (threadPool.isTerminated()) {
      initializeThreadPool();
    }
  }

  public synchronized void stop() {
    for (Updater updater : updaters) {
      updater.shutdown();
    }
    threadPool.shutdown();
    try {
      if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
        log.warn("Threadpool timed out on await termination - jobs still running!");
      }
    } catch (InterruptedException e) {
      log.error("Interrupted shutting down!", e);
    }
  }

  public void renormalize() {
    for (int topic: topics) {
      topicTermCounts.assignRow(topic, topicTermCounts.viewRow(topic).normalize(1));
      topicSums.assign(1.0);
    }
  }

  public Vector trainDocTopicModel(Vector original, int[] labels, int[] topics,Matrix docTopicModel, boolean inf) {
    // first calculate p(topic|term,document) for all terms in original, and all topics,
    // using p(term|topic) and p(topic|doc)
    List<Integer> terms = new ArrayList<Integer>();
    Iterator<Vector.Element> docElementIter = original.iterateNonZero();
    double docTermCount = 0.0;
    while (docElementIter.hasNext()) {
      Vector.Element element = docElementIter.next();
      terms.add(element.index());
      docTermCount += element.get();
    }
    pTopicGivenTerm(terms, labels, docTopicModel);
    normByTopicAndMultiByCount(original, terms, docTopicModel);
    Vector result=new RandomAccessSparseVector(topics.length);
    if (inf) {
      for (int topic : topics) {
        result.set(topic, (docTopicModel.viewRow(topic).norm(1) + alpha) / (docTermCount + numTerms * alpha));
      }
      result.assign(Functions.mult(1 / result.norm(1)));
    }
    return result;
  }

  public void update(Matrix docTopicCounts) {
    for (int topic: topics) {
      updaters[topic % updaters.length].update(topic, docTopicCounts.viewRow(topic));
    }
  }

  public void updateTopic(int topic, Vector docTopicCounts) {
    //log.info("get iterateNonZero");
    Iterator<Vector.Element> docTopicElementIter = docTopicCounts.iterateNonZero();
    //log.info("got iterateNonZero");
    Vector distTopicTermCountRow = topicTermCounts.viewRow(topic);
    double topicCountSum = 0.0;
    StringBuilder builder = new StringBuilder();
    //int num = 0;
    //log.info("enter while");
    while (docTopicElementIter.hasNext()) {
      Vector.Element topicTermCount = docTopicElementIter.next();
      //num++;
      int termIndex = topicTermCount.index();
      double count = topicTermCount.get();
      //builder.append(termIndex + ":" + count + ",");
      //if (num > 50) {
      //  log.info("num increase to 50,vector is {}", builder.toString());
      //}
      topicCountSum += count;
      distTopicTermCountRow.setQuick(termIndex, count + distTopicTermCountRow.getQuick(termIndex));
    }
    //log.info("topic: {}; docTopicCounts: {}", new Object[]{topic, builder.toString()});
    topicSums.set(topic, topicSums.get(topic) + topicCountSum);
  }

  public void update(int termId, Vector topicCounts) {
    for (int topic: topics) {
      Vector v = topicTermCounts.viewRow(topic);
      v.set(termId, v.get(termId) + topicCounts.get(topic));
    }
    topicSums.assign(topicCounts, Functions.PLUS);
  }

  public void persist(Path outputDir, boolean overwrite) throws IOException {
    FileSystem fs = outputDir.getFileSystem(conf);
    if (overwrite) {
      fs.delete(outputDir, true); // CHECK second arg
    }
    DistributedRowMatrixWriter.write(outputDir, conf, topicTermCounts);
  }

  /*
     if infer according to labels ;
     topicWeitht should use labels.get(topicIndex) while now use topicTermCounts to get it
     then the inf can be executed more than once,but there is unsafety because notLabeled url's count may be bigger than labeled
 */

  private void pTopicGivenTerm(List<Integer> terms, int[] topicLabels, Matrix termTopicDist) {
    int modelTermSize = topicTermCounts.columnSize();
    double Vbeta = eta * numTerms;
    for (Integer topicIndex : topicLabels) {
      Vector termTopicRow = termTopicDist.viewRow(topicIndex);
      Vector topicTermRow = topicTermCounts.viewRow(topicIndex);
      double topicSum = topicSums.getQuick(topicIndex);
      double docTopicSum = 0.0;
      for (Integer termIndex : terms) {
        if (termIndex >= modelTermSize)
          continue;
        docTopicSum += topicTermRow.getQuick(termIndex);
      }
      for (Integer termIndex : terms) {
        if (termIndex >= modelTermSize)
          continue;
        double topicTermCount = topicTermRow.getQuick(termIndex);
        double topicWeight = docTopicSum - topicTermCount;
        double termTopicLikelihood = (topicTermCount + eta) * (topicWeight + alpha) / (topicSum + Vbeta);
        termTopicRow.setQuick(termIndex, termTopicLikelihood);
      }
    }
  }


  /**
   * \(sum_x sum_a (c_ai * log(p(x|i) * p(a|x)))\)
   */
  public double perplexity(Vector document, Vector docTopics) {
    double perplexity = 0;
    double norm = docTopics.norm(1) + (docTopics.size() * alpha);
    Iterator<Vector.Element> docElementIter = document.iterateNonZero();
    while (docElementIter.hasNext()) {
      Vector.Element e = docElementIter.next();
      int term = e.index();
      double prob = 0;
      for (int topic: topics) {
        double d = (docTopics.getQuick(topic) + alpha) / norm;
        double p = d * (topicTermCounts.viewRow(topic).get(term) + eta)
          / (topicSums.get(topic) + eta * numTerms);
        prob += p;
      }
      perplexity += e.get() * Math.log(prob);
    }
    return -perplexity;
  }

  private void normByTopicAndMultiByCount(Vector doc, List<Integer> terms, Matrix perTopicSparseDistributions) {
    // then make sure that each of these is properly normalized by topic: sum_x(p(x|t,d)) = 1
    for (Integer termIndex : terms) {
      double sum = 0;
      for (int topic: topics) {
        sum += perTopicSparseDistributions.viewRow(topic).getQuick(termIndex);
      }
      double count = doc.getQuick(termIndex);
      for (int topic: topics) {
        perTopicSparseDistributions.viewRow(topic).setQuick(termIndex,
          perTopicSparseDistributions.viewRow(topic).getQuick(termIndex) * count / sum);
      }
    }
  }

  public static String vectorToSortedString(Vector vector, String[] dictionary) {
    List<Pair<String, Double>> vectorValues = Lists.newArrayListWithCapacity(vector.getNumNondefaultElements());
    Iterator<Vector.Element> elementIter = vector.iterateNonZero();
    while (elementIter.hasNext()) {
      Vector.Element e = elementIter.next();
      vectorValues.add(Pair.of(dictionary != null ? dictionary[e.index()] : String.valueOf(e.index()),
        e.get()));
    }
    Collections.sort(vectorValues, new Comparator<Pair<String, Double>>() {
      @Override
      public int compare(Pair<String, Double> x, Pair<String, Double> y) {
        return y.getSecond().compareTo(x.getSecond());
      }
    });
    Iterator<Pair<String, Double>> listIt = vectorValues.iterator();
    StringBuilder bldr = new StringBuilder(2048);
    bldr.append('{');
    int i = 0;
    while (listIt.hasNext() && i < 25) {
      i++;
      Pair<String, Double> p = listIt.next();
      bldr.append(p.getFirst());
      bldr.append(':');
      bldr.append(p.getSecond());
      bldr.append(',');
    }
    if (bldr.length() > 1) {
      bldr.setCharAt(bldr.length() - 1, '}');
    }
    return bldr.toString();
  }

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  private final class Updater implements Runnable {
    private final ArrayBlockingQueue<Pair<Integer, Vector>> queue =
      new ArrayBlockingQueue<Pair<Integer, Vector>>(500);
    private boolean shutdown = false;
    private boolean shutdownComplete = false;

    public void shutdown() {
      try {
        synchronized (this) {
          while (!shutdownComplete) {
            shutdown = true;
            wait(10000L); // Arbitrarily, wait 10 seconds rather than forever for this
          }
        }
      } catch (InterruptedException e) {
        log.warn("Interrupted waiting to shutdown() : ", e);
      }
    }

    public boolean update(int topic, Vector v) {
      if (shutdown) { // maybe don't do this?
        throw new IllegalStateException("In SHUTDOWN state: cannot submit tasks");
      }
      while (true) { // keep trying if interrupted
        try {
          // start async operation by submitting to the queue
          if (queue.offer(Pair.of(topic, v), 3, TimeUnit.SECONDS)) {
            //log.info("queue size increase to {}", queue.size());
            // return once you got access to the queue
            return true;
          }else {
            Thread.sleep(100);
          }
        } catch (InterruptedException e) {
          log.warn("Interrupted trying to queue update:", e);
        }
      }
    }

    @Override
    public void run() {
      while (!shutdown) {
        try {
          //long t1=System.currentTimeMillis();
          Pair<Integer, Vector> pair = queue.poll(1,TimeUnit.SECONDS);
          //log.info("queue size decrease to {}", queue.size());
          if (pair != null) {
            //long t2 = System.currentTimeMillis();
            //log.info("start updateTopic {}", pair.getSecond().size());
            updateTopic(pair.getFirst(), pair.getSecond());
            //log.info("updateTopic use {} ms", (System.currentTimeMillis() - t2));
          }
          //log.info("update pair use {} ms",(System.currentTimeMillis()-t1));
        } catch (InterruptedException e) {
          log.warn("Interrupted waiting to poll for update", e);
        }
      }
      // in shutdown mode, finish remaining tasks!
      for (Pair<Integer, Vector> pair : queue) {
        updateTopic(pair.getFirst(), pair.getSecond());
      }
      synchronized (this) {
        shutdownComplete = true;
        notifyAll();
      }
    }
  }


}
