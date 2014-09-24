package com.elex.bigdata.llda.mahout.model;

import com.elex.bigdata.llda.mahout.math.SparseRowDenseColumnMatrix;
import com.elex.bigdata.llda.mahout.math.SparseRowSparseColumnMatrix;
import com.elex.bigdata.llda.mahout.math.SparseRowSqSparseColumnMatrix;
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
import java.util.Arrays;
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

/**
 *  训练和推理的模型
 */
public class LabeledTopicModel implements Configurable, Iterable<MatrixSlice> {

  private static final Logger log = LoggerFactory.getLogger(LabeledTopicModel.class);
  // 字典的路径
  private final String[] dictionary;
  // 最核心的模型，一个以topic_label为行号，以word index为列号的矩阵。
  private final AbstractMatrix topicTermCounts;
  // 记录各个主题下的单词数
  private final Vector topicSums;
  // 所有的主题
  private final int[] topics;
  private final int numTopics;
  private final int numTerms;
  // 两个先验参数
  private final double eta;
  private final double alpha;

  private Configuration conf;

  private final Sampler sampler;
  // 训练的线程数
  private final int numThreads;
  private ThreadPoolExecutor threadPool;
  // 新模型的更新线程
  private Updater[] updaters;
  //指示什么时候能够打log
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
    //assert topicSums.size() > 100;
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
  // 开启训练和模型更新线程
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
  // produce random model according to topics and numTerms
  // 一般在训练的第一次迭代过程之前，由于没有已经生成的模型，需要随机生成初始模型。
  private static Pair<AbstractMatrix, Vector> randomMatrix(int[] topics, int numTerms, Random random) {
    AbstractMatrix topicTermCounts = new SparseRowDenseColumnMatrix(MathUtil.getMax(topics) + 1, numTerms);
    Vector topicSums = new DenseVector(MathUtil.getMax(topics) + 1);
    if (random != null) {
      for (int topic : topics) {
        for (int term = 0; term < numTerms; term++) {
          topicTermCounts.set(topic, term, random.nextDouble());
        }
      }
    }
    for (int topic : topics) {
      topicSums.setQuick(topic, random == null ? 1.0 : topicTermCounts.viewRow(topic).norm(1));
    }
    assert topicTermCounts.rowSize() > 100;
    return Pair.of(topicTermCounts, topicSums);
  }
  // 从hdfs中载入模型;实际上就是从一个sequenceFile 中载入topicLabel:wordCountVector 序列。
  // 根据这些组成topicTermCountMatrix，并且计算各个主题中的单词数
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
    AbstractMatrix model = new SparseRowDenseColumnMatrix(numTopics, numTerms);
    Vector topicSums = new DenseVector(numTopics);
    for (Pair<Integer, Vector> pair : rows) {
      int topic = pair.getFirst();
      model.assignRow(topic, pair.getSecond());
      //System.out.println("topic:" + topic + ",sum:" + pair.getSecond().norm(1.0));
      //计算该主题中单词总数
      double sum = model.viewRow(topic).norm(1.0);
      topicSums.setQuick(topic, sum);
      //log.info("topic " + topic + " sum: " + sum);
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
      topicTermCounts.assignRow(topic, new DenseVector(numTerms));
    }
    topicSums.assign(1.0);
    if (threadPool.isTerminated()) {
      initializeThreadPool();
    }
  }
  //停止各个训练和模型更新线程
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
  // train doc with labels
  // 每一篇文档（由orignal 和labels组成）都要经过训练后得出该文档中单词的主题分布，即docTopicTermModel
  public void trainDocTopicModel(Vector original, int[] labels, Matrix docTopicTermModel) {
    // first calculate p(topic|term,document) for all terms in original, and all topics,
    // using p(term|topic) and p(topic|doc)
    trainNum++;
    long t1 = System.nanoTime();
    // 由于对vector的遍历相对比较慢一点，所以从vector中提取出terms和counts来。
    List<Integer> terms = new ArrayList<Integer>();
    List<Double>  counts= new ArrayList<Double>();
    Iterator<Vector.Element> docElementIter = original.iterateNonZero();
    while (docElementIter.hasNext()) {
      Vector.Element element = docElementIter.next();
      if (element.index() < topicTermCounts.columnSize())
      {
        terms.add(element.index());
        counts.add(element.get());
      }
    }
    double[] termSums=new double[terms.size()];
    Arrays.fill(termSums,0.0);
    //计算term-topic概率分布
    pTopicGivenTerm(terms, labels, docTopicTermModel,termSums);
    /*
    if(trainNum%100000 == 1){
      log.info(Thread.currentThread().getName()+"  trainNum {} ",trainNum );
      StringBuilder builder=new StringBuilder();
      for(double termSum: termSums){
        builder.append(termSum+" , ");
      }
      //log.info("termSums: "+builder.toString());
    }*/
    //对刚刚计算每个单词的概率分布归一化，并乘以单词的数量
    normByTopicAndMultiByCount(counts,termSums,labels,docTopicTermModel);
    long t2 = System.nanoTime();
    /*
    if (trainNum % 100000 == 1) {
      StringBuilder builder=new StringBuilder();
      for(int label: labels)
        builder.append(label+",");
      StringBuilder docBuilder=new StringBuilder();
      for(int i=0;i<terms.size();i++){
        docBuilder.append(terms.get(i)+":"+counts.get(i)+",");
      }
      log.info(Thread.currentThread().getName()+"  train use " + (t2 - t1) / (1000) + " us, doc: "+docBuilder.toString() );
      log.info(Thread.currentThread().getName()+"  labels: "+builder.toString());
      StringBuilder builder1=new StringBuilder();
      double allTopicProbSum=0.0;
      for( int label: labels){
        double topicProb=docTopicModel.viewRow(label).norm(1.0);
        allTopicProbSum+=topicProb;
        builder1.append("label "+label+": sum "+topicProb+" , ");
      }
      log.info(Thread.currentThread().getName()+"  "+builder1.toString());
      log.info(Thread.currentThread().getName()+"  allTopicProbSum "+allTopicProbSum);
      log.info(Thread.currentThread().getName()+"  "+"train complete");
    }*/
  }

  /**
   * @param orignal
   * @param labels
   * @return
   * 在已有模型的前提下，推理出文档的主题分布
   * doc.p[topic]=(doc.count[topic]+alpha)/(doc.count+topics.length*alpha)
   */
  public Vector inf(Vector orignal, int[] labels) {
    Matrix docTopicTermDist = new SparseRowSqSparseColumnMatrix(numTopics, orignal.size());
    //计算出文档中各单词的主题分布
    trainDocTopicModel(orignal, labels, docTopicTermDist);
    Vector result = new DenseVector(numTopics);
    double docTermCount = orignal.norm(1.0);
    //计算各个主题的概率
    for (int topic : topics) {
      result.set(topic, (docTopicTermDist.viewRow(topic).norm(1) + alpha) / (docTermCount + topics.length * alpha));
    }
    //概率归一化
    result.assign(Functions.mult(1 / result.norm(1)));
    return result;
  }

  /**
   * @param docTopicTermCounts
   * 将推理出来的单词概率矩阵更新到矩阵topicTermCount中去。
   * 在实际的训练过程中，我们在依据数据训练时所根据的模型矩阵叫做readModel，而更新的那个模型叫做writeModel，见LabeledModelTrainer
   * update的过程如下：首先将topic（即matrixSlice.index())，termCountsVector(即matrixSlice.vector()）放入队列中，
   * 然后Updater将取出来并调用updateTopic函数更新到矩阵中去。
   */
  public void update(Matrix docTopicTermCounts) {
    Iterator<MatrixSlice> iter = docTopicTermCounts.iterator();
    updateNum++;
    while (iter.hasNext()) {
      MatrixSlice matrixSlice = iter.next();
      updaters[updateNum % updaters.length].update(matrixSlice.index(), matrixSlice.vector());
    }
  }

  /**
   * @param topic
   * @param termCounts
   * 更新模型矩阵topicTermCounts中的topic这一行。termCounts是训练某一个文档的结果，
   */

  public void updateTopic(int topic, Vector termCounts) {
    //log.info("get iterateNonZero");
    //long t1=System.nanoTime();
    Iterator<Vector.Element> docTopicElementIter = termCounts.iterateNonZero();
    //log.info("got iterateNonZero");
    Vector globalTermCounts = topicTermCounts.viewRow(topic);
    double topicCountSum = 0.0;
    while (docTopicElementIter.hasNext()) {
      Vector.Element topicTermCount = docTopicElementIter.next();
      int termIndex = topicTermCount.index();
      double count = topicTermCount.get();
      topicCountSum += count;
      globalTermCounts.setQuick(termIndex, count + globalTermCounts.getQuick(termIndex));
    }

    //topicTermCounts.assignRow(topic, globalTermCounts);
    //log.info("topic: {}; docTopicCounts: {}", new Object[]{topic, builder.toString()});
    topicSums.setQuick(topic, topicSums.getQuick(topic) + topicCountSum);
    /*
    long t2=System.nanoTime();

    if(updateNum%100000==1){
      StringBuilder builder=new StringBuilder();
      Iterator<Vector.Element> iterator = termCounts.iterateNonZero();
      while(iterator.hasNext()){
        Vector.Element e=iterator.next();
        builder.append(e.index()+":"+e.get()+",");
      }
      log.info(Thread.currentThread().getName()+"  updateNum " + updateNum);
      log.info(Thread.currentThread().getName()+"  topic: "+topic+", termCounts: "+builder.toString());
      log.info(Thread.currentThread().getName()+"  updateTopic: "+topic+"  "+topicTermCounts.viewRow(topic).norm(1.0)+" "+globalTermCounts.norm(1.0)+" docSize "+termCounts.size());
      log.info(Thread.currentThread().getName()+"  updateTopic use : "+(t2-t1)/1000 +" us");
    }*/
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

  /**
   *
   * @param terms
   * @param topicLabels
   * @param termTopicDist
   * @param termSum
   * 计算terms中各个单词的主题概率分布情况，计算的公式为
   * p[topicIndex]（即termTopicLikelihood)正比于(weight[topic]+alpha)*(doc.count[topic]+beta)/(totalCount[topic]+V*beta)
   * 因为上面计算出来的概率值还需要归一化才能算作真正的概率，所以计算termSum是为了下一步normByTopicAndMultiByCount 对term-topic prob进行归一化做准备。
   */
  private void pTopicGivenTerm(List<Integer> terms, int[] topicLabels, Matrix termTopicDist,double[] termSum) {
    double Vbeta = eta * numTerms;
    for (Integer topicIndex : topicLabels) {
      Vector termTopicRow = termTopicDist.viewRow(topicIndex);
      Vector topicTermRow = topicTermCounts.viewRow(topicIndex);
      double topicSum = topicSums.getQuick(topicIndex);
      double docTopicSum = 0.0;
      for (Integer termIndex : terms) {
        docTopicSum += topicTermRow.getQuick(termIndex);
      }
      for (int i=0;i<terms.size();i++) {
        int termIndex=terms.get(i);
        double topicTermCount = topicTermRow.getQuick(termIndex);
        double topicWeight = docTopicSum - topicTermCount;
        double termTopicLikelihood = (topicTermCount + eta) * (topicWeight + alpha) / (topicSum + Vbeta);
        termTopicRow.setQuick(termIndex, termTopicLikelihood);
        termSum[i]+=termTopicLikelihood;
      }
      //termTopicDist.assignRow(topicIndex, termTopicRow);
    }
  }


  /**
   * 在已知文档主题分布的情况下，计算某篇文档的复杂度(perplexity)
   * \(sum_x sum_a (c_ai * log(p(x|i) * p(a|x)))\)
   */
  public double perplexity(Vector document, Vector docTopics) {
    double perplexity = 0;
    double norm = docTopics.norm(1) + (docTopics.size() * alpha);
    Iterator<Vector.Element> docElementIter = document.iterateNonZero();
    while (docElementIter.hasNext()) {
      Vector.Element e = docElementIter.next();
      int term = e.index();
      if(term>getNumTerms())
        continue;
      //prob为选择单词term的概率
      //计算办法为p(term)=sum(p(topic,term) for each topic )
      double prob = 0;
      for (int topic : topics) {
        //d为选择主题topic的概率
        double d = (docTopics.getQuick(topic) + alpha) / norm;
        //p为选择主题topic且在topic下选择单词term的概率
        double p = d * (topicTermCounts.viewRow(topic).get(term) + eta)
          / (topicSums.get(topic) + eta * numTerms);
        prob += p;
      }
      //该单词的复杂度为-count(即e.get())×log(prob）
      perplexity += e.get() * Math.log(prob);
    }
    return -perplexity;
  }

  /**
   *
   * @param counts
   * @param termSums
   * @param labels
   * @param perTopicSparseDistributions
   * perTopicSparseDistributions 和termSums都是在上一步中计算来的
   * 这个函数用来对perTopicSparseDistributions 进行归一化和
   */
  private void normByTopicAndMultiByCount(List<Double> counts, double[] termSums,int[] labels,Matrix perTopicSparseDistributions) {
    // then make sure that each of these is properly normalized by topic: sum_x(p(x|t,d)) = 1
    for(int topic: labels){
      Vector termDist=perTopicSparseDistributions.viewRow(topic);
      int i=0;
      Iterator<Vector.Element> iter=termDist.iterateNonZero();
      while(iter.hasNext()){
        Vector.Element e=iter.next();
        //除以termSums[i]是归一化，而乘以counts.get(i)则是为了使得这个值表示处于该主题的单词的个数
        e.set(e.get()*counts.get(i)/termSums[i]);
        i++;
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
