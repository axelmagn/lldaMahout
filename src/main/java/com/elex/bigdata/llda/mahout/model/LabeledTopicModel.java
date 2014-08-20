package com.elex.bigdata.llda.mahout.model;

import com.elex.bigdata.llda.mahout.util.MathUtil;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
  private final int numTopics;
  private final int numTerms;
  private final double eta;
  private final double alpha;

  private Configuration conf;

  private final Sampler sampler;
  private final int numThreads;
  private ThreadPoolExecutor threadPool;
  private Updater[] updaters;

  private int trainNum = 0;
  private int updateNum = 0;
  public int getNumTerms() {
    return numTerms;
  }

  public int[] getTopics() {
    return topics;
  }

  public int getNumTopics() {
    return numTopics;
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

  public LabeledTopicModel(AbstractMatrix topicTermCounts, Vector topicSums, double eta, double alpha,
                           String[] dictionary, int numThreads, double modelWeight) {
    this.dictionary = dictionary;
    this.topicTermCounts = topicTermCounts;
    this.topicSums = topicSums;
    this.topics = new int[topicSums.size()];
    Iterator<Vector.Element> iter = topicSums.iterateNonZero();
    int i = 0;
    while (iter.hasNext()) {
      topics[i++] = iter.next().index();
    }
    numTopics = MathUtil.getMax(topics) + 1;
    assert topicSums.size() > 100;
    this.numTerms = topicTermCounts.numCols();
    this.eta = eta;
    this.alpha = alpha;
    this.sampler = new Sampler(RandomUtils.getRandom());
    this.numThreads = numThreads;
    if (modelWeight != 1) {
      topicSums.assign(Functions.mult(modelWeight));
      for (int topic : topics) {
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
    AbstractMatrix topicTermCounts = new SparseMatrix(MathUtil.getMax(topics) + 1, numTerms);
    Vector topicSums = new RandomAccessSparseVector(MathUtil.getMax(topics) + 1);
    if (random != null) {
      for (int topic : topics) {
        for (int term = 0; term < numTerms; term++) {
          topicTermCounts.set(topic, term, random.nextDouble());
        }
      }
    }
    for (int topic : topics) {
      topicTermCounts.assignRow(topic, topicTermCounts.viewRow(topic));
      topicSums.setQuick(topic, random == null ? 1.0 : topicTermCounts.viewRow(topic).norm(1));
    }
    assert topicTermCounts.rowSize() > 100;
    return Pair.of(topicTermCounts, topicSums);
  }

  public static Pair<AbstractMatrix, Vector> loadModel(Configuration conf, Path... modelPaths)
    throws IOException {
    int numTerms = -1;
    int numTopics = 0;
    List<Pair<Integer, Vector>> rows = Lists.newArrayList();
    for (Path modelPath : modelPaths) {
      log.info("load model from {}", modelPath.toString());
      for (Pair<IntWritable, VectorWritable> row
        : new SequenceFileIterable<IntWritable, VectorWritable>(modelPath, true, conf)) {
        rows.add(Pair.of(row.getFirst().get(), row.getSecond().get()));
        if (row.getFirst().get() > numTopics)
          numTopics = row.getFirst().get();
        if (numTerms < 0) {
          numTerms = row.getSecond().get().size();
        }
      }
    }
    numTopics++;
    if (rows.isEmpty()) {
      throw new IOException(java.util.Arrays.toString(modelPaths) + " have no vectors in it");
    }
    log.info("numTopics is {},numTerms is {}", numTopics, numTerms);
    AbstractMatrix model = new SparseMatrix(numTopics, numTerms);
    Vector topicSums = new RandomAccessSparseVector(numTopics);
    for (Pair<Integer, Vector> pair : rows) {
      int topic = pair.getFirst();
      model.assignRow(topic, pair.getSecond());
      System.out.println("topic:" + topic + ",sum:" + pair.getSecond().norm(1.0));
      double sum = model.viewRow(topic).norm(1.0);
      topicSums.setQuick(topic, sum);
      log.info("topic " + topic + " sum: " + sum);
    }
    //assert model.rowSize()>100;
    return Pair.of(model, topicSums);
  }

  // NOTE: this is purely for debug purposes.  It is not performant to "toString()" a real model
  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    for (int topic : topics) {
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
    for (int topic : topics) {
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
    for (int topic : topics) {
      topicTermCounts.assignRow(topic, topicTermCounts.viewRow(topic).normalize(1));
      topicSums.assign(1.0);
    }
  }

  public void trainDocTopicModel(Vector original, int[] labels, Matrix docTopicModel) {
    // first calculate p(topic|term,document) for all terms in original, and all topics,
    // using p(term|topic) and p(topic|doc)
    trainNum++;
    long t1 = System.nanoTime();
    List<Integer> terms = new ArrayList<Integer>();
    Iterator<Vector.Element> docElementIter = original.iterateNonZero();
    while (docElementIter.hasNext()) {
      Vector.Element element = docElementIter.next();
      if (element.index() < topicTermCounts.columnSize())
        terms.add(element.index());
    }
    pTopicGivenTerm(terms, labels, docTopicModel);
    normByTopicAndMultiByCount(original, terms, docTopicModel);
    long t2 = System.nanoTime();
    if (trainNum % 5000 == 1) {
      log.info("trainNum: ",trainNum);
      log.info("train use " + (t2 - t1) / (1000) + " us");
    }
  }

  public Vector inf(Vector orignal, int[] labels) {
    Matrix docTopicTermDist = new SparseMatrix(numTopics, orignal.size());
    for (int label : labels)
      docTopicTermDist.assignRow(label, new RandomAccessSparseVector(orignal.size()));
    trainDocTopicModel(orignal, labels, docTopicTermDist);
    Vector result = new RandomAccessSparseVector(numTopics);
    double docTermCount = orignal.norm(1.0);
    for (int topic : topics) {
      result.set(topic, (docTopicTermDist.viewRow(topic).norm(1) + alpha) / (docTermCount + numTerms * alpha));
    }
    result.assign(Functions.mult(1 / result.norm(1)));
    return result;
  }

  public void update(Matrix docTopicCounts) {
    Iterator<MatrixSlice> iter = docTopicCounts.iterator();
    updateNum++;
    while (iter.hasNext()) {
      MatrixSlice matrixSlice = iter.next();
      updaters[updateNum % updaters.length].update(matrixSlice.index(), matrixSlice.vector());
    }
  }

  public void updateTopic(int topic, Vector termCounts) {
    //log.info("get iterateNonZero");
    long t1=System.nanoTime();
    Iterator<Vector.Element> docTopicElementIter = termCounts.iterateNonZero();
    //log.info("got iterateNonZero");
    Vector globalTermCounts = topicTermCounts.viewRow(topic);
    double topicCountSum = 0.0;
    while (docTopicElementIter.hasNext()) {
      Vector.Element topicTermCount = docTopicElementIter.next();
      int termIndex = topicTermCount.index();
      double count = topicTermCount.get();
      if(updateNum%5000==1)
        log.info(termIndex+":"+count);
      topicCountSum += count;
      globalTermCounts.setQuick(termIndex, count + globalTermCounts.getQuick(termIndex));
    }

    topicTermCounts.assignRow(topic, globalTermCounts);
    //log.info("topic: {}; docTopicCounts: {}", new Object[]{topic, builder.toString()});
    topicSums.setQuick(topic, topicSums.getQuick(topic) + topicCountSum);
    long t2=System.nanoTime();
    if(updateNum%5000==1){
      log.info("updateNum "+updateNum);
      log.info("updateTopic: "+topicTermCounts.viewRow(topic).norm(1.0)+" "+globalTermCounts.norm(1.0)+" docSize "+termCounts.size());
      log.info("updateTopic use : "+(t2-t1)/1000 +" us");
    }
  }

  public void update(int termId, Vector topicCounts) {
    for (int topic : topics) {
      topicTermCounts.setQuick(topic, termId, topicTermCounts.getQuick(topic, termId) + topicCounts.get(topic));
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
    double Vbeta = eta * numTerms;
    for (Integer topicIndex : topicLabels) {
      Vector termTopicRow = termTopicDist.viewRow(topicIndex);
      Vector topicTermRow = topicTermCounts.viewRow(topicIndex);
      double topicSum = topicSums.getQuick(topicIndex);
      double docTopicSum = 0.0;
      for (Integer termIndex : terms) {
        docTopicSum += topicTermRow.getQuick(termIndex);
      }
      for (Integer termIndex : terms) {
        double topicTermCount = topicTermRow.getQuick(termIndex);
        double topicWeight = docTopicSum - topicTermCount;
        double termTopicLikelihood = (topicTermCount + eta) * (topicWeight + alpha) / (topicSum + Vbeta);
        termTopicRow.setQuick(termIndex, termTopicLikelihood);
      }
      //termTopicDist.assignRow(topicIndex, termTopicRow);
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
      for (int topic : topics) {
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
      for (int topic : topics) {
        sum += perTopicSparseDistributions.viewRow(topic).getQuick(termIndex);
      }
      double count = doc.getQuick(termIndex);
      for (int topic : topics) {
        double orig = perTopicSparseDistributions.getQuick(topic, termIndex);
        perTopicSparseDistributions.setQuick(topic, termIndex, orig * count / sum);
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
          } else {
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
          Pair<Integer, Vector> pair = queue.poll(1, TimeUnit.SECONDS);
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
