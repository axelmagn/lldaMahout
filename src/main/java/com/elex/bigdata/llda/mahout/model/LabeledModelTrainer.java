package com.elex.bigdata.llda.mahout.model;

import com.elex.bigdata.llda.mahout.util.MathUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.lda.cvb.TopicModel;
import org.apache.mahout.math.*;
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.Arrays;
import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/15/14
 * Time: 4:37 PM
 * To change this template use File | Settings | File Templates.
 */
public class LabeledModelTrainer {
  private static final Logger log = LoggerFactory.getLogger(LabeledModelTrainer.class);

  private final int numTerms;
  private LabeledTopicModel readModel;
  private LabeledTopicModel writeModel;
  private ThreadPoolExecutor threadPool;
  private BlockingQueue<Runnable> workQueue;
  private final int numTrainThreads;
  private final boolean isReadWrite;
  private final int[] topics;

  public LabeledModelTrainer(LabeledTopicModel initialReadModel, LabeledTopicModel initialWriteModel,
                             int numTrainThreads, int[] topics, int numTerms) {
    this.readModel = initialReadModel;
    this.writeModel = initialWriteModel;
    this.numTrainThreads = numTrainThreads;
    this.topics=topics;
    this.numTerms = numTerms;
    isReadWrite = initialReadModel == initialWriteModel;
  }

  /**
   * WARNING: this constructor may not lead to good behavior.  What should be verified is that
   * the model updating process does not conflict with model reading.  It might work, but then
   * again, it might not!
   *
   * @param model           to be used for both reading (inference) and accumulating (learning)
   * @param numTrainThreads
   * @param topics
   * @param numTerms
   */
  public LabeledModelTrainer(LabeledTopicModel model, int numTrainThreads, int[] topics, int numTerms) {
    this(model, model, numTrainThreads, topics, numTerms);
  }

  public LabeledTopicModel getReadModel() {
    return readModel;
  }

  public void start() {
    log.info("Starting training threadpool with {} threads", numTrainThreads);
    workQueue = new ArrayBlockingQueue<Runnable>(numTrainThreads * 100);
    threadPool = new ThreadPoolExecutor(numTrainThreads, numTrainThreads, 0, TimeUnit.SECONDS,
      workQueue);
    threadPool.allowCoreThreadTimeOut(false);
    threadPool.prestartAllCoreThreads();
    writeModel.reset();
  }

  public void train(VectorIterable matrix, VectorIterable docTopicCounts, boolean isInf) {
    train(matrix, docTopicCounts, topics, isInf);
  }

  public double calculatePerplexity(VectorIterable matrix, Iterable<int[]> docTopicLabels) {
    return calculatePerplexity(matrix,docTopicLabels, 0);
  }
  public double calculatePerplexity(Vector doc,int[] labels){
    Vector topicDist=readModel.inf(doc, labels);
    double perplexity = readModel.perplexity(doc, topicDist);
    return perplexity;
  }

  public double calculatePerplexity(VectorIterable matrix, Iterable<int[]> docTopicCounts,
                                    double testFraction) {
    Iterator<MatrixSlice> docIterator = matrix.iterator();
    Iterator<int[]> labelsIterator = docTopicCounts.iterator();
    double perplexity = 0;
    double matrixNorm = 0;
    while (docIterator.hasNext() && labelsIterator.hasNext()) {
      MatrixSlice docSlice = docIterator.next();
      int[] topicLabels = labelsIterator.next();
      int docId = docSlice.index();
      Vector document = docSlice.vector();
      if (testFraction == 0 || docId % (1 / testFraction) == 0) {
        Vector topicDist=readModel.inf(document, topicLabels);
        perplexity += readModel.perplexity(document, topicDist);
        matrixNorm += document.norm(1);
      }
    }
    return perplexity / matrixNorm;
  }

  public void train(VectorIterable matrix, VectorIterable docLabels, int[] topics, boolean inf) {
    start();
    Iterator<MatrixSlice> docIterator = matrix.iterator();
    Iterator<MatrixSlice> docTopicIterator = docLabels.iterator();
    long startTime = System.nanoTime();
    int i = 0;
    double[] times = new double[100];
    Map<Vector, int[]> batch = Maps.newHashMap();
    int numTokensInBatch = 0;
    long batchStart = System.nanoTime();
    while (docIterator.hasNext() && docTopicIterator.hasNext()) {
      i++;
      Vector document = docIterator.next().vector();
      Vector labelVector = docTopicIterator.next().vector();
      Iterator<Vector.Element> iterator=labelVector.iterateNonZero();
      List<Integer> labelList=new ArrayList<Integer>();
      while(iterator.hasNext()){
        labelList.add(iterator.next().index());
      }
      int[] labels=new int[labelList.size()];
      int j=0;
      for(int label:labelList){
        labels[j++]=label;
      }
      if (isReadWrite) {
        if (batch.size() < numTrainThreads) {

          batch.put(document, labels);
          if (log.isDebugEnabled()) {
            numTokensInBatch += document.getNumNondefaultElements();
          }
        } else {
          batchTrain(batch,true);
          long time = System.nanoTime();
          log.debug("trained " + numTrainThreads + "docs with " + numTokensInBatch + " tokens, start time " + batchStart + ", end time " + time);
          batchStart = time;
          numTokensInBatch = 0;
        }
      } else {
        long start = System.nanoTime();
        train(document, labels, true);
        if (log.isDebugEnabled()) {
          times[i % times.length] =
            (System.nanoTime() - start) / (1.0e6 * document.getNumNondefaultElements());
          if (i % 100 == 0) {
            long time = System.nanoTime() - startTime;
            log.debug("trained {} documents in {}ms", i, time / 1.0e6);
            if (i % 500 == 0) {
              Arrays.sort(times);
              log.debug("training took median {}ms per token-instance", times[times.length / 2]);
            }
          }
        }
      }
    }
    stop();
  }



  public void batchTrain(Map<Vector, int[]> batch,boolean update) {
    while (true) {
      try {
        List<TrainerRunnable> runnables = Lists.newArrayList();
        for (Map.Entry<Vector, int[]> entry : batch.entrySet()) {
          runnables.add(new TrainerRunnable(readModel, null, entry.getKey(),entry.getValue()));
        }
        threadPool.invokeAll(runnables);
        if (update) {
          for (TrainerRunnable runnable : runnables) {
            writeModel.update(runnable.docTopicModel);
          }
        }
        break;
      } catch (InterruptedException e) {
        log.warn("Interrupted during batch training, retrying!", e);
      }
    }
  }

  public void train(Vector document, int[] labels, boolean update) {
    while (true) {
      try {
        workQueue.put(new TrainerRunnable(readModel, update? writeModel: null, document, labels));
        //log.info("workQueue size {}", workQueue.size());
        return;
      } catch (InterruptedException e) {
        log.warn("Interrupted waiting to submit document to work queue: {}", document, e);
      }
    }
  }

  public void trainSync(Vector document, int[] labels, boolean update) {
    new TrainerRunnable(readModel, update ? writeModel : null, document, labels).run();
  }


  public void stop() {
    long startTime = System.nanoTime();
    log.info("Initiating stopping of training threadpool");
    try {
      threadPool.shutdown();
      if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
        log.warn("Threadpool timed out on await termination - jobs still running!");
      }
      long newTime = System.nanoTime();
      log.info("threadpool took: {}ms", (newTime - startTime) / 1.0e6);
      startTime = newTime;
      readModel.stop();
      newTime = System.nanoTime();
      log.info("readModel.stop() took {}ms", (newTime - startTime) / 1.0e6);
      startTime = newTime;
      writeModel.stop();
      newTime = System.nanoTime();
      log.info("writeModel.stop() took {}ms", (newTime - startTime) / 1.0e6);
      LabeledTopicModel tmpModel = writeModel;
      writeModel = readModel;
      readModel = tmpModel;
    } catch (InterruptedException e) {
      log.error("Interrupted shutting down!", e);
    }
  }

  public void persist(Path outputPath) throws IOException {
    readModel.persist(outputPath, true);
  }

  private static final class TrainerRunnable implements Runnable, Callable<Double> {
    private final LabeledTopicModel readModel;
    private final LabeledTopicModel writeModel;
    private final Vector document;
    private final int[] labels;
    private final Matrix docTopicModel;

    private TrainerRunnable(LabeledTopicModel readModel, LabeledTopicModel writeModel, Vector document,
                            int[] labels) {
      this.readModel = readModel;
      this.writeModel = writeModel;
      this.document = document;
      this.labels = labels;
      this.docTopicModel = new SparseMatrix(readModel.getNumTopics(),document.size());
    }


    @Override
    public void run() {
      readModel.trainDocTopicModel(document, labels, docTopicModel);
      if (writeModel != null) {
        writeModel.update(docTopicModel);
      }
    }

    @Override
    public Double call() {
      run();
      Vector docTopics=readModel.inf(document,labels);
      return readModel.perplexity(document, docTopics);
    }

  }
}
