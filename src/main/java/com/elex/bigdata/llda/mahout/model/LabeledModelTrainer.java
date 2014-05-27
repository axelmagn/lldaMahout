package com.elex.bigdata.llda.mahout.model;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.lda.cvb.TopicModel;
import org.apache.mahout.math.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

  private final int numTopics;
  private final int numTerms;
  private LabeledTopicModel readModel;
  private LabeledTopicModel writeModel;
  private ThreadPoolExecutor threadPool;
  private BlockingQueue<Runnable> workQueue;
  private final int numTrainThreads;
  private final boolean isReadWrite;

  public LabeledModelTrainer(LabeledTopicModel initialReadModel, LabeledTopicModel initialWriteModel,
                      int numTrainThreads, int numTopics, int numTerms) {
    this.readModel = initialReadModel;
    this.writeModel = initialWriteModel;
    this.numTrainThreads = numTrainThreads;
    this.numTopics = numTopics;
    this.numTerms = numTerms;
    isReadWrite = initialReadModel == initialWriteModel;
  }

  /**
   * WARNING: this constructor may not lead to good behavior.  What should be verified is that
   * the model updating process does not conflict with model reading.  It might work, but then
   * again, it might not!
   * @param model to be used for both reading (inference) and accumulating (learning)
   * @param numTrainThreads
   * @param numTopics
   * @param numTerms
   */
  public LabeledModelTrainer(LabeledTopicModel model, int numTrainThreads, int numTopics, int numTerms) {
    this(model, model, numTrainThreads, numTopics, numTerms);
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

  public void train(VectorIterable matrix, VectorIterable docTopicCounts,boolean isInf) {
    train(matrix, docTopicCounts, 1,isInf);
  }

  public double calculatePerplexity(VectorIterable matrix, VectorIterable docTopicCounts) {
    return calculatePerplexity(matrix, docTopicCounts, 0);
  }

  public double calculatePerplexity(VectorIterable matrix, VectorIterable docTopicCounts,
                                    double testFraction) {
    Iterator<MatrixSlice> docIterator = matrix.iterator();
    Iterator<MatrixSlice> docTopicIterator = docTopicCounts.iterator();
    double perplexity = 0;
    double matrixNorm = 0;
    while (docIterator.hasNext() && docTopicIterator.hasNext()) {
      MatrixSlice docSlice = docIterator.next();
      MatrixSlice topicSlice = docTopicIterator.next();
      int docId = docSlice.index();
      Vector document = docSlice.vector();
      Vector topicDist = topicSlice.vector();
      if (testFraction == 0 || docId % (1 / testFraction) == 0) {
        trainSync(document, topicDist, false, 10,true);
        perplexity += readModel.perplexity(document, topicDist);
        matrixNorm += document.norm(1);
      }
    }
    return perplexity / matrixNorm;
  }

  public void train(VectorIterable matrix, VectorIterable docTopicCounts, int numDocTopicIters,boolean inf) {
    start();
    Iterator<MatrixSlice> docIterator = matrix.iterator();
    Iterator<MatrixSlice> docTopicIterator = docTopicCounts.iterator();
    long startTime = System.nanoTime();
    int i = 0;
    double[] times = new double[100];
    Map<Vector, Vector> batch = Maps.newHashMap();
    int numTokensInBatch = 0;
    long batchStart = System.nanoTime();
    while (docIterator.hasNext() && docTopicIterator.hasNext()) {
      i++;
      Vector document = docIterator.next().vector();
      Vector topicDist = docTopicIterator.next().vector();
      if (isReadWrite) {
        if (batch.size() < numTrainThreads) {
          batch.put(document, topicDist);
          if (log.isDebugEnabled()) {
            numTokensInBatch += document.getNumNondefaultElements();
          }
        } else {
          batchTrain(batch, true, numDocTopicIters,inf);
          long time = System.nanoTime();
          log.debug("trained "+numTrainThreads+"docs with "+numTokensInBatch+" tokens, start time "+batchStart+", end time "+time);
          batchStart = time;
          numTokensInBatch = 0;
        }
      } else {
        long start = System.nanoTime();
        train(document, topicDist, true, numDocTopicIters,inf);
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

  public void batchTrain(Map<Vector, Vector> batch, boolean update, int numDocTopicsIters,boolean isInf) {
    while (true) {
      try {
        List<TrainerRunnable> runnables = Lists.newArrayList();
        for (Map.Entry<Vector, Vector> entry : batch.entrySet()) {
          runnables.add(new TrainerRunnable(readModel, null, entry.getKey(),
            entry.getValue(), new SparseRowMatrix(numTopics, numTerms, true),
            numDocTopicsIters,isInf));
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

  public void train(Vector document, Vector docTopicCounts, boolean update, int numDocTopicIters,boolean isInf) {
    while (true) {
      try {
        workQueue.put(new TrainerRunnable(readModel, update
          ? writeModel
          : null, document, docTopicCounts, new SparseRowMatrix(numTopics, document.size(), true), numDocTopicIters,isInf));
        return;
      } catch (InterruptedException e) {
        log.warn("Interrupted waiting to submit document to work queue: {}", document, e);
      }
    }
  }

  public void trainSync(Vector document, Vector docTopicCounts, boolean update,
                        int numDocTopicIters,boolean isInf) {
    new TrainerRunnable(readModel, update
      ? writeModel
      : null, document, docTopicCounts, new SparseRowMatrix(numTopics, numTerms, true), numDocTopicIters,isInf).run();
  }

  public double calculatePerplexity(Vector document, Vector docTopicCounts, int numDocTopicIters) {
    TrainerRunnable runner =  new TrainerRunnable(readModel, null, document, docTopicCounts,
      new SparseRowMatrix(numTopics, document.size(), true), numDocTopicIters,true);
    return runner.call();
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
    private final Vector docTopics;
    private final Matrix docTopicModel;
    private final int numDocTopicIters;
    private boolean isInf;

    private TrainerRunnable(LabeledTopicModel readModel, LabeledTopicModel writeModel, Vector document,
                            Vector docTopics, Matrix docTopicModel, int numDocTopicIters,boolean isInf) {
      this.readModel = readModel;
      this.writeModel = writeModel;
      this.document = document;
      this.docTopics = docTopics;
      this.docTopicModel = docTopicModel;
      this.numDocTopicIters = numDocTopicIters;
      this.isInf=isInf;
    }

    @Override
    public void run() {
      //for (int i = 0; i < numDocTopicIters; i++) {
        // synchronous read-only call:
        //long t1=System.currentTimeMillis();
        readModel.trainDocTopicModel(document, docTopics, docTopicModel,isInf);
        //long t2=System.currentTimeMillis();
        //log.info("trainerRunnable run use "+(t2-t1)+" ms");
      //}
      if (writeModel != null) {
        // parallel call which is read-only on the docTopicModel, and write-only on the writeModel
        // this method does not return until all rows of the docTopicModel have been submitted
        // to write work queues
        writeModel.update(docTopicModel);
      }
      //log.info("update use "+(System.currentTimeMillis()-t2)+" ms");
    }

    @Override
    public Double call() {
      run();
      return readModel.perplexity(document, docTopics);
    }
  }
}
