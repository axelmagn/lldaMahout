package com.elex.bigdata.llda.mahout.mapreduce.est;

import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver;
import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocReducer;
import com.elex.bigdata.llda.mahout.data.inputformat.CombineSqInputFormat;
import com.elex.bigdata.llda.mahout.data.inputformat.CombineSqRecordReader;
import com.elex.bigdata.llda.mahout.mapreduce.inf.LLDAInferenceMapper;
import com.elex.bigdata.llda.mahout.util.FileSystemUtil;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/22/14
 * Time: 5:36 PM
 * To change this template use File | Settings | File Templates.
 */
public class LLDADriver extends AbstractJob {
  private static final Logger log = LoggerFactory.getLogger(LLDADriver.class);

  public static final String NUM_TOPICS = "num_topics";
  public static final String NUM_TERMS = "num_terms";
  public static final String DOC_TOPIC_SMOOTHING = "doc_topic_smoothing";
  public static final String TERM_TOPIC_SMOOTHING = "term_topic_smoothing";
  public static final String DICTIONARY = "dictionary";
  public static final String DOC_TOPIC_OUTPUT = "doc_topic_output";
  public static final String MODEL_TEMP_DIR = "topic_model_temp_dir";
  public static final String ITERATION_BLOCK_SIZE = "iteration_block_size";
  public static final String RANDOM_SEED = "random_seed";
  public static final String TEST_SET_FRACTION = "test_set_fraction";
  public static final String NUM_TRAIN_THREADS = "num_train_threads";
  public static final String NUM_UPDATE_THREADS = "num_update_threads";
  public static final String MAX_ITERATIONS_PER_DOC = "max_doc_topic_iters";
  public static final String MODEL_WEIGHT = "prev_iter_mult";
  public static final String NUM_REDUCE_TASKS = "num_reduce_tasks";
  public static final String BACKFILL_PERPLEXITY = "backfill_perplexity";
  private static final String MODEL_PATHS = "mahout.lda.cvb.modelPath";

  private static final double DEFAULT_CONVERGENCE_DELTA = 0.0005;
  private static final double DEFAULT_DOC_TOPIC_SMOOTHING = 0.6;
  private static final double DEFAULT_TERM_TOPIC_SMOOTHING = 0.08;
  private static final int DEFAULT_ITERATION_BLOCK_SIZE = 10;
  private static final double DEFAULT_TEST_SET_FRACTION = 0;
  private static final int DEFAULT_NUM_TRAIN_THREADS = 4;
  private static final int DEFAULT_NUM_UPDATE_THREADS = 1;
  private static final int DEFAULT_MAX_ITERATIONS_PER_DOC = 10;
  private static final int DEFAULT_NUM_REDUCE_TASKS = 10;

  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption(GenerateLDocDriver.RESOURCE_ROOT,"rDir","specify the resources Dir",true);
    addOption(DefaultOptionCreator.maxIterationsOption().create());
    addOption(DefaultOptionCreator.CONVERGENCE_DELTA_OPTION, "cd", "The convergence delta value",
      String.valueOf(DEFAULT_CONVERGENCE_DELTA));
    addOption(DefaultOptionCreator.overwriteOption().create());

    addOption(NUM_TOPICS, "k", "Number of topics to learn", true);
    addOption(NUM_TERMS, "nt", "Vocabulary size", false);
    addOption(DOC_TOPIC_SMOOTHING, "a", "Smoothing for document/topic distribution",
      String.valueOf(DEFAULT_DOC_TOPIC_SMOOTHING));
    addOption(TERM_TOPIC_SMOOTHING, "e", "Smoothing for topic/term distribution",
      String.valueOf(DEFAULT_TERM_TOPIC_SMOOTHING));
    addOption(DICTIONARY, "dict", "Path to term-dictionary file(s) (glob expression supported)", false);
    addOption(DOC_TOPIC_OUTPUT, "dt", "Output path for the training doc/topic distribution", false);
    addOption(MODEL_TEMP_DIR, "mt", "Path to intermediate model path (useful for restarting)", false);
    addOption(ITERATION_BLOCK_SIZE, "block", "Number of iterations per perplexity check",
      String.valueOf(DEFAULT_ITERATION_BLOCK_SIZE));
    addOption(RANDOM_SEED, "seed", "Random seed", false);
    addOption(TEST_SET_FRACTION, "tf", "Fraction of data to hold out for testing",
      String.valueOf(DEFAULT_TEST_SET_FRACTION));
    addOption(NUM_TRAIN_THREADS, "ntt", "number of threads per mapper to train with",
      String.valueOf(DEFAULT_NUM_TRAIN_THREADS));
    addOption(NUM_UPDATE_THREADS, "nut", "number of threads per mapper to update the model with",
      String.valueOf(DEFAULT_NUM_UPDATE_THREADS));
    addOption(MAX_ITERATIONS_PER_DOC, "mipd", "max number of iterations per doc for p(topic|doc) learning",
      String.valueOf(DEFAULT_MAX_ITERATIONS_PER_DOC));
    addOption(NUM_REDUCE_TASKS, null, "number of reducers to use during model estimation",
      String.valueOf(DEFAULT_NUM_REDUCE_TASKS));
    addOption(buildOption(BACKFILL_PERPLEXITY, null, "enable backfilling of missing perplexity values", false, false,
      null));

    if (parseArguments(args) == null) {
      return -1;
    }

    int numTopics = Integer.parseInt(getOption(NUM_TOPICS));
    Path inputPath = getInputPath();
    Path topicModelOutputPath = getOutputPath();
    int maxIterations = Integer.parseInt(getOption(DefaultOptionCreator.MAX_ITERATIONS_OPTION));
    int iterationBlockSize = Integer.parseInt(getOption(ITERATION_BLOCK_SIZE));
    double convergenceDelta = Double.parseDouble(getOption(DefaultOptionCreator.CONVERGENCE_DELTA_OPTION));
    double alpha = Double.parseDouble(getOption(DOC_TOPIC_SMOOTHING));
    double eta = Double.parseDouble(getOption(TERM_TOPIC_SMOOTHING));
    int numTrainThreads = Integer.parseInt(getOption(NUM_TRAIN_THREADS));
    int numUpdateThreads = Integer.parseInt(getOption(NUM_UPDATE_THREADS));
    int maxItersPerDoc = Integer.parseInt(getOption(MAX_ITERATIONS_PER_DOC));
    Path dictionaryPath = hasOption(DICTIONARY) ? new Path(getOption(DICTIONARY)) : null;
    int numTerms = hasOption(NUM_TERMS)
      ? Integer.parseInt(getOption(NUM_TERMS))
      : getNumTerms(getConf(), dictionaryPath);
    Path docTopicOutputPath = hasOption(DOC_TOPIC_OUTPUT) ? new Path(getOption(DOC_TOPIC_OUTPUT)) : null;
    Path modelTempPath = hasOption(MODEL_TEMP_DIR)
      ? new Path(getOption(MODEL_TEMP_DIR))
      : getTempPath("topicModelState");
    long seed = hasOption(RANDOM_SEED)
      ? Long.parseLong(getOption(RANDOM_SEED))
      : System.nanoTime() % 10000;
    float testFraction = hasOption(TEST_SET_FRACTION)
      ? Float.parseFloat(getOption(TEST_SET_FRACTION))
      : 0.0f;
    int numReduceTasks = Integer.parseInt(getOption(NUM_REDUCE_TASKS));
    boolean backfillPerplexity = hasOption(BACKFILL_PERPLEXITY);

    Configuration conf=getConf();
    conf.set(GenerateLDocDriver.RESOURCE_ROOT,getOption(GenerateLDocDriver.RESOURCE_ROOT));

    return run(conf, inputPath, topicModelOutputPath, numTopics, numTerms, alpha, eta,
      maxIterations, iterationBlockSize, convergenceDelta, dictionaryPath, docTopicOutputPath,
      modelTempPath, seed, testFraction, numTrainThreads, numUpdateThreads, maxItersPerDoc,
      numReduceTasks, backfillPerplexity);
  }

  public static int[] getTopics(Configuration conf) throws IOException {

    Map<Integer,Integer> child2ParentLabels= GenerateLDocReducer.getLabelRelations(conf);
    int[] topics=new int[child2ParentLabels.size()];
    int i=0;
    for(Integer topicLabel: child2ParentLabels.keySet())
      topics[i++]=topicLabel;
    return topics;
  }

  private static int getNumTerms(Configuration conf, Path dictionaryPath) throws IOException {
    FileSystem fs = dictionaryPath.getFileSystem(conf);
    Text key = new Text();
    IntWritable value = new IntWritable();
    int maxTermId = -1;
    for (FileStatus stat : fs.globStatus(dictionaryPath)) {
      SequenceFile.Reader reader = new SequenceFile.Reader(fs, stat.getPath(), conf);
      while (reader.next(key, value)) {
        maxTermId = Math.max(maxTermId, value.get());
      }
    }
    return maxTermId + 1;
  }

  public static int run(Configuration conf,
                        Path inputPath,
                        Path topicModelOutputPath,
                        int numTopics,
                        int numTerms,
                        double alpha,
                        double eta,
                        int maxIterations,
                        int iterationBlockSize,
                        double convergenceDelta,
                        Path dictionaryPath,
                        Path docTopicOutputPath,
                        Path topicModelStateTempPath,
                        long randomSeed,
                        float testFraction,
                        int numTrainThreads,
                        int numUpdateThreads,
                        int maxItersPerDoc,
                        int numReduceTasks,
                        boolean backfillPerplexity)
    throws ClassNotFoundException, IOException, InterruptedException {
    // verify arguments
    Preconditions.checkArgument(testFraction >= 0.0 && testFraction <= 1.0,
      "Expected 'testFraction' value in range [0, 1] but found value '%s'", testFraction);
    Preconditions.checkArgument(!backfillPerplexity || testFraction > 0.0,
      "Expected 'testFraction' value in range (0, 1] but found value '%s'", testFraction);

    String infoString = "Will run Collapsed Variational Bayes (0th-derivative approximation) "
      + "learning for LDA on {} (numTerms: {}), finding {}-topics, with document/topic prior {}, "
      + "topic/term prior {}.  Maximum iterations to run will be {}, unless the change in "
      + "perplexity is less than {}.  Topic model output (p(term|topic) for each topic) will be "
      + "stored {}.  Random initialization seed is {}, holding out {} of the data for perplexity "
      + "check\n";
    log.info(infoString, new Object[] {inputPath, numTerms, numTopics, alpha, eta, maxIterations,
      convergenceDelta, topicModelOutputPath, randomSeed, testFraction});
    infoString = dictionaryPath == null
      ? "" : "Dictionary to be used located " + dictionaryPath.toString() + '\n';
    infoString += docTopicOutputPath == null
      ? "" : "p(topic|docId) will be stored " + docTopicOutputPath.toString() + '\n';
    log.info(infoString);

    FileSystem fs = FileSystem.get(topicModelStateTempPath.toUri(), conf);
    int iterationNumber = getCurrentIterationNumber(conf, topicModelStateTempPath, maxIterations);
    log.info("Current iteration number: {}", iterationNumber);

    conf.set(NUM_TOPICS, String.valueOf(numTopics));
    conf.set(NUM_TERMS, String.valueOf(numTerms));
    conf.set(DOC_TOPIC_SMOOTHING, String.valueOf(alpha));
    conf.set(TERM_TOPIC_SMOOTHING, String.valueOf(eta));
    conf.set(RANDOM_SEED, String.valueOf(randomSeed));
    conf.set(NUM_TRAIN_THREADS, String.valueOf(numTrainThreads));
    conf.set(NUM_UPDATE_THREADS, String.valueOf(numUpdateThreads));
    conf.set(MAX_ITERATIONS_PER_DOC, String.valueOf(maxItersPerDoc));
    conf.set(MODEL_WEIGHT, "1"); // TODO
    conf.set(TEST_SET_FRACTION, String.valueOf(testFraction));
    conf.set("mapred.map.child.java.opts","-Xss3036k -Xmx4596m");
    conf.set("mapred.reduce.child.java.opts","-Xmx6192m");

    List<Double> perplexities = Lists.newArrayList();
    for (int i = 1; i <= iterationNumber; i++) {
      // form path to model
      Path modelPath = modelPath(topicModelStateTempPath, i);

      // read perplexity
      double perplexity = readPerplexity(conf, topicModelStateTempPath, i);
      if (Double.isNaN(perplexity)) {
        if (!(backfillPerplexity && i % iterationBlockSize == 0)) {
          continue;
        }
        log.info("Backfilling perplexity at iteration {}", i);
        if (!fs.exists(modelPath)) {
          log.error("Model path '{}' does not exist; Skipping iteration {} perplexity calculation", modelPath.toString(), i);
          continue;
        }
        perplexity = calculatePerplexity(conf, inputPath, modelPath, i);
      }

      // register and log perplexity
      perplexities.add(perplexity);
      log.info("Perplexity at iteration {} = {}", i, perplexity);
    }

    long startTime = System.currentTimeMillis();
    while (iterationNumber < maxIterations) {
      // test convergence
      if (convergenceDelta > 0.0) {
        double delta = rateOfChange(perplexities);
        if (delta < convergenceDelta) {
          log.info("Convergence achieved at iteration {} with perplexity {} and delta {}",
            new Object[]{iterationNumber, perplexities.get(perplexities.size() - 1), delta});
          break;
        }
      }

      // update model
      iterationNumber++;
      log.info("About to run iteration {} of {}", iterationNumber, maxIterations);
      Path modelInputPath = modelPath(topicModelStateTempPath, iterationNumber - 1);
      Path modelOutputPath = modelPath(topicModelStateTempPath, iterationNumber);
      /*
      if(iterationNumber==1){
        InitTopicTermModelDriver.runJob(conf,inputPath,modelInputPath);
        perplexities.add(calculatePerplexity(conf, inputPath, modelInputPath, iterationNumber-1));
      }
      */
      runIteration(conf, inputPath, modelInputPath, modelOutputPath, iterationNumber,
        maxIterations, numReduceTasks);

      // calculate perplexity
      if (testFraction > 0 && iterationNumber % iterationBlockSize == 0) {
        perplexities.add(calculatePerplexity(conf, inputPath, modelOutputPath, iterationNumber));
        log.info("Current perplexity = {}", perplexities.get(perplexities.size() - 1));
        log.info("(p_{} - p_{}) / p_0 = {}; target = {}", new Object[]{
          iterationNumber , iterationNumber - iterationBlockSize, rateOfChange(perplexities), convergenceDelta
        });
      }
    }
    log.info("Completed {} iterations in {} seconds", iterationNumber,
      (System.currentTimeMillis() - startTime)/1000);
    log.info("Perplexities: ({})", Joiner.on(", ").join(perplexities));

    // write final topic-term and doc-topic distributions
    Path finalIterationData = modelPath(topicModelStateTempPath, iterationNumber);
    Job topicModelOutputJob = topicModelOutputPath != null
      ? writeTopicModel(conf, finalIterationData, topicModelOutputPath)
      : null;
    if (topicModelOutputJob != null && !topicModelOutputJob.waitForCompletion(true)) {
      return -1;
    }
    Job docInferenceJob = docTopicOutputPath != null
      ? writeDocTopicInference(conf, inputPath, finalIterationData, docTopicOutputPath)
      : null;

    if (docInferenceJob != null && !docInferenceJob.waitForCompletion(true)) {
      return -1;
    }
    return 0;
  }

  private static double rateOfChange(List<Double> perplexities) {
    int sz = perplexities.size();
    if (sz < 2) {
      return Double.MAX_VALUE;
    }
    return Math.abs(perplexities.get(sz - 1) - perplexities.get(sz - 2)) / perplexities.get(0);
  }

  private static double calculatePerplexity(Configuration conf, Path corpusPath, Path modelPath, int iteration)
    throws IOException,
    ClassNotFoundException, InterruptedException {
    String jobName = "Calculating perplexity for " + modelPath;
    log.info("About to run: " + jobName);
    Job job = new Job(conf, jobName);
    job.setJarByClass(CachingLLDAPerplexityMapper.class);
    job.setMapperClass(CachingLLDAPerplexityMapper.class);
    job.setCombinerClass(DualDoubleSumReducer.class);
    job.setReducerClass(DualDoubleSumReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(DoubleWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.addInputPath(job, corpusPath);
    Path outputPath = perplexityPath(modelPath.getParent(), iteration);
    FileOutputFormat.setOutputPath(job, outputPath);
    setModelPaths(job, modelPath);
    HadoopUtil.delete(conf, outputPath);
    if (!job.waitForCompletion(true)) {
      throw new InterruptedException("Failed to calculate perplexity for: " + modelPath);
    }
    return readPerplexity(conf, modelPath.getParent(), iteration);
  }

  /**
   * Sums keys and values independently.
   */
  public static class DualDoubleSumReducer extends
    Reducer<DoubleWritable, DoubleWritable, DoubleWritable, DoubleWritable> {
    private final DoubleWritable outKey = new DoubleWritable();
    private final DoubleWritable outValue = new DoubleWritable();

    @Override
    public void run(Context context) throws IOException,
      InterruptedException {
      double keySum = 0.0;
      double valueSum = 0.0;
      while (context.nextKey()) {
        keySum += context.getCurrentKey().get();
        for (DoubleWritable value : context.getValues()) {
          valueSum += value.get();
        }
      }
      outKey.set(keySum);
      outValue.set(valueSum);
      context.write(outKey, outValue);
    }
  }

  /**
   * @param topicModelStateTemp
   * @param iteration
   * @return {@code double[2]} where first value is perplexity and second is model weight of those
   *         documents sampled during perplexity computation, or {@code null} if no perplexity data
   *         exists for the given iteration.
   * @throws IOException
   */
  public static double readPerplexity(Configuration conf, Path topicModelStateTemp, int iteration)
    throws IOException {
    Path perplexityPath = perplexityPath(topicModelStateTemp, iteration);
    FileSystem fs = FileSystem.get(perplexityPath.toUri(), conf);
    if (!fs.exists(perplexityPath)) {
      log.warn("Perplexity path {} does not exist, returning NaN", perplexityPath);
      return Double.NaN;
    }
    double perplexity = 0;
    double modelWeight = 0;
    long n = 0;
    for (Pair<DoubleWritable, DoubleWritable> pair : new SequenceFileDirIterable<DoubleWritable, DoubleWritable>(
      perplexityPath, PathType.LIST, PathFilters.partFilter(), null, true, conf)) {
      modelWeight += pair.getFirst().get();
      perplexity += pair.getSecond().get();
      n++;
    }
    log.info("Read {} entries with total perplexity {} and model weight {}", new Object[] { n,
      perplexity, modelWeight });
    return perplexity / modelWeight;
  }

  private static Job writeTopicModel(Configuration conf, Path modelInput, Path output) throws IOException,
    InterruptedException, ClassNotFoundException {
    String jobName = String.format("Writing final topic/term distributions from %s to %s", modelInput,
      output);
    log.info("About to run: " + jobName);
    Job job = new Job(conf, jobName);
    job.setJarByClass(LLDADriver.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapperClass(LLDATopicTermVectorMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(VectorWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.addInputPath(job, modelInput);
    FileOutputFormat.setOutputPath(job, output);
    FileSystem fs=FileSystem.get(conf);
    if(fs.exists(output))
      fs.delete(output);
    job.submit();
    return job;
  }

  private static Job writeDocTopicInference(Configuration conf, Path corpus, Path modelInput, Path output)
    throws IOException, ClassNotFoundException, InterruptedException {
    String jobName = String.format("Writing final document/topic inference from %s to %s", corpus,
      output);
    log.info("About to run: " + jobName);
    Job job = new Job(conf, jobName);
    job.setMapperClass(LLDAInferenceMapper.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    FileSystem fs = FileSystem.get(corpus.toUri(), conf);
    if (modelInput != null && fs.exists(modelInput)) {
      FileStatus[] statuses = fs.listStatus(modelInput, PathFilters.partFilter());
      URI[] modelUris = new URI[statuses.length];
      for (int i = 0; i < statuses.length; i++) {
        modelUris[i] = statuses[i].getPath().toUri();
      }
      DistributedCache.setCacheFiles(modelUris, conf);
    }
    if(fs.exists(output)){
      fs.delete(output);
    }
    setModelPaths(job,modelInput);
    FileInputFormat.addInputPath(job, corpus);
    FileOutputFormat.setOutputPath(job, output);
    job.setJarByClass(LLDADriver.class);
    job.submit();
    return job;
  }

  public static Path modelPath(Path topicModelStateTempPath, int iterationNumber) {
    return new Path(topicModelStateTempPath, "model-" + iterationNumber);
  }

  public static Path stage1OutputPath(Path topicModelStateTempPath, int iterationNumber) {
    return new Path(topicModelStateTempPath, "tmp-" + iterationNumber);
  }

  public static Path perplexityPath(Path topicModelStateTempPath, int iterationNumber) {
    return new Path(topicModelStateTempPath, "perplexity-" + iterationNumber);
  }

  private static int getCurrentIterationNumber(Configuration config, Path modelTempDir, int maxIterations)
    throws IOException {
    FileSystem fs = FileSystem.get(modelTempDir.toUri(), config);
    int iterationNumber = 1;
    Path iterationPath = modelPath(modelTempDir, iterationNumber);
    while (fs.exists(iterationPath) && iterationNumber <= maxIterations) {
      log.info("Found previous state: " + iterationPath);
      iterationNumber++;
      iterationPath = modelPath(modelTempDir, iterationNumber);
    }
    return iterationNumber - 1;
  }

  public static void runIteration(Configuration conf, Path corpusInput, Path modelInput, Path modelOutput,
                                  int iterationNumber, int maxIterations, int numReduceTasks) throws IOException, ClassNotFoundException, InterruptedException {
    String jobName = String.format("Iteration %d of %d, model input path: %s, input path: %s",
      iterationNumber, maxIterations, modelInput,corpusInput);
    log.info("About to run: " + jobName);
    FileSystemUtil.setCombineInputSplitSize(conf,corpusInput);
    Job job = new Job(conf, jobName);
    job.setJarByClass(LLDADriver.class);
    job.setMapperClass(LLDAMapper.class);
    job.setCombinerClass(VectorSumReducer.class);
    job.setReducerClass(VectorSumReducer.class);
    job.setNumReduceTasks(numReduceTasks);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(VectorWritable.class);
    job.setInputFormatClass(CombineSqInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.addInputPath(job, corpusInput);
    FileOutputFormat.setOutputPath(job, modelOutput);
    setModelPaths(job, modelInput);
    HadoopUtil.delete(conf, modelOutput);
    if (!job.waitForCompletion(true)) {
      throw new InterruptedException(String.format("Failed to complete iteration %d stage 1",
        iterationNumber));
    }
  }

  public static void setModelPaths(Job job, Path modelPath) throws IOException {
    Configuration conf = job.getConfiguration();
    if (modelPath == null || !FileSystem.get(modelPath.toUri(), conf).exists(modelPath)) {
      return;
    }
    FileStatus[] statuses = FileSystem.get(modelPath.toUri(), conf).listStatus(modelPath, PathFilters.partFilter());
    Preconditions.checkState(statuses.length > 0, "No part files found in model path '%s'", modelPath.toString());
    String[] modelPaths = new String[statuses.length];
    for (int i = 0; i < statuses.length; i++) {
      modelPaths[i] = statuses[i].getPath().toUri().toString();
    }
    conf.setStrings(MODEL_PATHS, modelPaths);
  }

  public static Path[] getModelPaths(Configuration conf) {
    String[] modelPathNames = conf.getStrings(MODEL_PATHS);
    if (modelPathNames == null || modelPathNames.length == 0) {
      return null;
    }
    Path[] modelPaths = new Path[modelPathNames.length];
    for (int i = 0; i < modelPathNames.length; i++) {
      modelPaths[i] = new Path(modelPathNames[i]);
    }
    return modelPaths;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new LLDADriver(), args);
  }

}
