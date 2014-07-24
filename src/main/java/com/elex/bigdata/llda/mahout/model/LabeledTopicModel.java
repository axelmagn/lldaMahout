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
  private final Matrix topicTermCounts;
  private final Vector topicSums;
  private final int numTopics;
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

  public int getNumTopics() {
    return numTopics;
  }

  public LabeledTopicModel(int numTopics, int numTerms, double eta, double alpha, String[] dictionary,
                           double modelWeight) {
    this(numTopics, numTerms, eta, alpha, null, dictionary, 1, modelWeight);
  }

  public LabeledTopicModel(Configuration conf, double eta, double alpha,
                           String[] dictionary, int numThreads, double modelWeight, Path... modelpath) throws IOException {
    this(loadModel(conf, modelpath), eta, alpha, dictionary, numThreads, modelWeight);
  }

  public LabeledTopicModel(int numTopics, int numTerms, double eta, double alpha, String[] dictionary,
                           int numThreads, double modelWeight) {
    this(new DenseMatrix(numTopics, numTerms), new DenseVector(numTopics), eta, alpha, dictionary,
      numThreads, modelWeight);
  }

  public LabeledTopicModel(int numTopics, int numTerms, double eta, double alpha, Random random,
                           String[] dictionary, int numThreads, double modelWeight) {
    this(randomMatrix(numTopics, numTerms, random), eta, alpha, dictionary, numThreads, modelWeight);
  }

  private LabeledTopicModel(Pair<Matrix, Vector> model, double eta, double alpha, String[] dict,
                            int numThreads, double modelWeight) {
    this(model.getFirst(), model.getSecond(), eta, alpha, dict, numThreads, modelWeight);
  }

  public LabeledTopicModel(Matrix topicTermCounts, Vector topicSums, double eta, double alpha,
                           String[] dictionary, double modelWeight) {
    this(topicTermCounts, topicSums, eta, alpha, dictionary, 1, modelWeight);
  }

  public LabeledTopicModel(Matrix topicTermCounts, double eta, double alpha, String[] dictionary,
                           int numThreads, double modelWeight) {
    this(topicTermCounts, viewRowSums(topicTermCounts),
      eta, alpha, dictionary, numThreads, modelWeight);
  }

  public LabeledTopicModel(Matrix topicTermCounts, Vector topicSums, double eta, double alpha,
                           String[] dictionary, int numThreads, double modelWeight) {
    this.dictionary = dictionary;
    this.topicTermCounts = topicTermCounts;
    this.topicSums = topicSums;
    this.numTopics = topicSums.size();
    this.numTerms = topicTermCounts.numCols();
    this.eta = eta;
    this.alpha = alpha;
    this.sampler = new Sampler(RandomUtils.getRandom());
    this.numThreads = numThreads;
    if (modelWeight != 1) {
      topicSums.assign(Functions.mult(modelWeight));
      for (int x = 0; x < numTopics; x++) {
        topicTermCounts.viewRow(x).assign(Functions.mult(modelWeight));
      }
    }
    initializeThreadPool();
  }

  private static Vector viewRowSums(Matrix m) {
    Vector v = new DenseVector(m.numRows());
    for (MatrixSlice slice : m) {
      v.set(slice.index(), slice.vector().norm(1));
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
    return topicTermCounts.iterateAll();
  }

  public Vector topicSums() {
    return topicSums;
  }

  private static Pair<Matrix, Vector> randomMatrix(int numTopics, int numTerms, Random random) {
    Matrix topicTermCounts = new DenseMatrix(numTopics, numTerms);
    Vector topicSums = new DenseVector(numTopics);
    if (random != null) {
      for (int x = 0; x < numTopics; x++) {
        for (int term = 0; term < numTerms; term++) {
          topicTermCounts.viewRow(x).set(term, random.nextDouble());
        }
      }
    }
    for (int x = 0; x < numTopics; x++) {
      topicSums.set(x, random == null ? 1.0 : topicTermCounts.viewRow(x).norm(1));
    }
    return Pair.of(topicTermCounts, topicSums);
  }

  public static Pair<Matrix, Vector> loadModel(Configuration conf, Path... modelPaths)
    throws IOException {
    int numTopics = -1;
    int numTerms = -1;
    List<Pair<Integer, Vector>> rows = Lists.newArrayList();
    for (Path modelPath : modelPaths) {
      log.info("load model from {}",modelPath.toString());
      for (Pair<IntWritable, VectorWritable> row
        : new SequenceFileIterable<IntWritable, VectorWritable>(modelPath, true, conf)) {
        rows.add(Pair.of(row.getFirst().get(), row.getSecond().get()));
        numTopics = Math.max(numTopics, row.getFirst().get());
        if (numTerms < 0) {
          numTerms = row.getSecond().get().size();
        }
      }
    }
    if (rows.isEmpty()) {
      throw new IOException(java.util.Arrays.toString(modelPaths) + " have no vectors in it");
    }
    numTopics++;
    log.info("numTopics is {},numTerms is {}",numTopics,numTerms);
    Matrix model = new DenseMatrix(numTopics, numTerms);
    Vector topicSums = new DenseVector(numTopics);
    for (Pair<Integer, Vector> pair : rows) {
      model.viewRow(pair.getFirst()).assign(pair.getSecond());
      topicSums.set(pair.getFirst(), pair.getSecond().norm(1));
    }
    return Pair.of(model, topicSums);
  }

  // NOTE: this is purely for debug purposes.  It is not performant to "toString()" a real model
  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    for (int x = 0; x < numTopics; x++) {
      String v = dictionary != null
        ? vectorToSortedString(topicTermCounts.viewRow(x).normalize(1), dictionary)
        : topicTermCounts.viewRow(x).asFormatString();
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
    for (int x = 0; x < numTopics; x++) {
      topicTermCounts.assignRow(x, new SequentialAccessSparseVector(numTerms));
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
    for (int x = 0; x < numTopics; x++) {
      topicTermCounts.assignRow(x, topicTermCounts.viewRow(x).normalize(1));
      topicSums.assign(1.0);
    }
  }

  public void trainDocTopicModel(Vector original, Vector labels, Matrix docTopicModel, boolean inf) {
    // first calculate p(topic|term,document) for all terms in original, and all topics,
    // using p(term|topic) and p(topic|doc)
    //log.info("before train. labels: " + labels.toString());
    //long preTime=System.currentTimeMillis();
    List<Integer> terms = new ArrayList<Integer>();
    Iterator<Vector.Element> docElementIter = original.iterateNonZero();
    //long getIterTime=System.currentTimeMillis();
    double docTermCount = 0.0;
    while (docElementIter.hasNext()) {
      Vector.Element element = docElementIter.next();
      terms.add(element.index());
      docTermCount+=element.get();
    }
    //long midTime=System.currentTimeMillis();
    List<Integer> topicLabels = new ArrayList<Integer>();
    Iterator<Vector.Element> labelIter=labels.iterateNonZero();
    while(labelIter.hasNext()){
      Vector.Element e=labelIter.next();
      topicLabels.add(e.index());
    }
    //long t1 = System.currentTimeMillis();
    //log.info("get List use {} ms ,with terms' size of {} and doc size of {},get term list use {} ms,get termIter use time {} ",new Object[]{(t1-preTime),terms.size(),original.size(),(midTime-preTime),(getIterTime-preTime)});
    //log.info("docTopicModel columns' length is {} ",docTopicModel.columnSize());
    pTopicGivenTerm(terms, topicLabels, docTopicModel);
    //long t2 = System.currentTimeMillis();
    //log.info("pTopic use {} ms with terms' size {}", new Object[]{(t2 - t1),terms.size()});
    normByTopicAndMultiByCount(original,terms,docTopicModel);
    //long t3 = System.currentTimeMillis();
    //log.info("normalize use {} ms with terms' size {}", new Object[]{(t3 - t2),terms.size()});
    // now multiply, term-by-term, by the document, to get the weighted distribution of
    // term-topic pairs from this document.

    // now recalculate \(p(topic|doc)\) by summing contributions from all of pTopicGivenTerm
    if (inf) {
      for (Vector.Element topic : labels) {
        labels.set(topic.index(), (docTopicModel.viewRow(topic.index()).norm(1) + alpha) / (docTermCount + numTerms * alpha));
      }
      // now renormalize so that \(sum_x(p(x|doc))\) = 1
      labels.assign(Functions.mult(1 / labels.norm(1)));
      //log.info("set topics use {} " + (System.currentTimeMillis() - t3));
    }
    //log.info("after train: "+ topics.toString());
  }

  public Vector infer(Vector original, Vector docTopics) {
    Vector pTerm = original.like();
    Iterator<Vector.Element> origElementIter = original.iterateNonZero();
    while (origElementIter.hasNext()) {
      Vector.Element e = origElementIter.next();
      int term = e.index();
      // p(a) = sum_x (p(a|x) * p(x|i))
      double pA = 0;
      Iterator<Vector.Element> topicElementIter = docTopics.iterateNonZero();
      while (topicElementIter.hasNext()) {
        Vector.Element topic = topicElementIter.next();
        pA += (topicTermCounts.viewRow(topic.index()).get(term) / topicSums.get(topic.index())) * docTopics.get(topic.index());
      }
      pTerm.set(term, pA);
    }
    return pTerm;
  }

  public void update(Matrix docTopicCounts) {
    for (int x = 0; x < numTopics; x++) {
      updaters[x % updaters.length].update(x, docTopicCounts.viewRow(x));
    }
  }

  public void updateTopic(int topic, Vector docTopicCounts) {
    Iterator<Vector.Element> docTopicElementIter=docTopicCounts.iterateNonZero();
    Vector distTopicTermCountRow=topicTermCounts.viewRow(topic);
    double topicCountSum=0.0;
    StringBuilder builder=new StringBuilder();
    while(docTopicElementIter.hasNext()){
      Vector.Element topicTermCount=docTopicElementIter.next();
      int termIndex=topicTermCount.index();
      double count=topicTermCount.get();
      builder.append(termIndex+":"+count+",");
      topicCountSum+=count;
      distTopicTermCountRow.setQuick(termIndex,count+distTopicTermCountRow.get(termIndex));
    }
    log.info("topic: {}; docTopicCounts: {}",new Object[]{topic,builder.toString()});
    topicSums.set(topic, topicSums.get(topic) + topicCountSum);
  }

  public void update(int termId, Vector topicCounts) {
    for (int x = 0; x < numTopics; x++) {
      Vector v = topicTermCounts.viewRow(x);
      v.set(termId, v.get(termId) + topicCounts.get(x));
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

  /**
   * Computes {@code \(p(topic x | term a, document i)\)} distributions given input document {@code i}.
   * {@code \(pTGT[x][a]\)} is the (un-normalized) {@code \(p(x|a,i)\)}, or if docTopics is {@code null},
   * {@code \(p(a|x)\)} (also un-normalized).
   *
   * @param document      doc-term vector encoding {@code \(w(term a|document i)\)}.
   * @param docTopics     {@code docTopics[x]} is the overall weight of topic {@code x} in given
   *                      document. If {@code null}, a topic weight of {@code 1.0} is used for all topics.
   * @param termTopicDist storage for output {@code \(p(x|a,i)\)} distributions.
   */
  private void pTopicGivenTerm(Vector document, Vector docTopics, Matrix termTopicDist) {
    // for each topic x
    /*
    for (int x = 0; x < numTopics; x++) {
      // get p(topic x | document i), or 1.0 if docTopics is null
      double topicWeight = docTopics == null ? 1.0 : docTopics.get(x);
      // get w(term a | topic x)
      Vector topicTermRow = topicTermCounts.viewRow(x);
      // get \sum_a w(term a | topic x)
      double topicSum = topicSums.get(x);
      // get p(topic x | term a) distribution to update
      Vector termTopicRow = termTopicDist.viewRow(x);

      // for each term a in document i with non-zero weight
      for (Vector.Element e : document.nonZeroes()) {
        int termIndex = e.index();

        // calc un-normalized p(topic x | term a, document i)
        double termTopicLikelihood = (topicTermRow.get(termIndex) + eta) * (topicWeight + alpha)
          / (topicSum + eta * numTerms);
        termTopicRow.set(termIndex, termTopicLikelihood);
      }
    }
    */
    int modelTermSize = topicTermCounts.columnSize();
    List<Integer> terms = new ArrayList<Integer>();
    Iterator<Vector.Element> docElementIter = document.iterateNonZero();
    while (docElementIter.hasNext()) {
      Vector.Element element = docElementIter.next();
      terms.add(element.index());
    }
    for (Vector.Element e : docTopics) {
      int topicIndex = e.index();
      Vector termTopicRow = termTopicDist.viewRow(topicIndex);
      if (e.get() == 0.0) {
        termTopicRow.assign(0.0);
        continue;
      }
      Vector topicTermRow = topicTermCounts.viewRow(topicIndex);
      double topicSum = topicSums.get(topicIndex);
      double docTopicSum = 0.0;
      for (Integer termIndex : terms) {
        docTopicSum += topicTermRow.get(termIndex);
      }

      for (Integer termIndex : terms) {
        if (termIndex > modelTermSize)
          continue;
        double topicWeight = docTopicSum - topicTermRow.get(termIndex);
        double termTopicLikelihood = (topicTermRow.get(termIndex) + eta) * (topicWeight + alpha) / (topicSum + eta * numTerms);
        termTopicRow.set(termIndex, termTopicLikelihood);
      }
    }
  }

  /*
     if infer according to labels ;
     topicWeitht should use labels.get(topicIndex) while now use topicTermCounts to get it
     then the inf can be executed more than once,but there is unsafety because notLabeled url's count may be bigger than labeled
 */

  private void pTopicGivenTerm(List<Integer> terms, List<Integer> topicLabels, Matrix termTopicDist) {
    int modelTermSize = topicTermCounts.columnSize();
    double Vbeta=eta*numTerms;
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
        double topicTermCount=topicTermRow.getQuick(termIndex);
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
      for (int x = 0; x < numTopics; x++) {
        double d = (docTopics.get(x) + alpha) / norm;
        double p = d * (topicTermCounts.viewRow(x).get(term) + eta)
          / (topicSums.get(x) + eta * numTerms);
        prob += p;
      }
      perplexity += e.get() * Math.log(prob);
    }
    return -perplexity;
  }

  private void normByTopicAndMultiByCount(Vector doc,List<Integer> terms,Matrix perTopicSparseDistributions) {
    // then make sure that each of these is properly normalized by topic: sum_x(p(x|t,d)) = 1
    for(Integer termIndex: terms){
      double sum = 0;
      for (int x = 0; x < numTopics; x++) {
        sum += perTopicSparseDistributions.viewRow(x).getQuick(termIndex);
      }
      double count=doc.getQuick(termIndex);
      for (int x = 0; x < numTopics; x++) {
        perTopicSparseDistributions.viewRow(x).setQuick(termIndex,
          perTopicSparseDistributions.viewRow(x).getQuick(termIndex)*count / sum);
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
          queue.put(Pair.of(topic, v));
          log.info("queue size increase to {}" ,queue.size());
          // return once you got access to the queue
          return true;
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
          log.info("queue size decrease to {}",queue.size());
          if (pair != null) {
            long t2=System.currentTimeMillis();
            updateTopic(pair.getFirst(), pair.getSecond());
            log.info("updateTopic use {} ms",(System.currentTimeMillis()-t2));
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
