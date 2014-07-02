package com.elex.bigdata.llda.mahout.mapreduce.inf;

import com.elex.bigdata.llda.mahout.mapreduce.est.LLDADriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static com.elex.bigdata.llda.mahout.mapreduce.est.LLDADriver.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/22/14
 * Time: 6:29 PM
 * To change this template use File | Settings | File Templates.
 */
public class LLDAInfDriver extends AbstractJob{
  private static final Logger log = LoggerFactory.getLogger(LLDAInfDriver.class);
  private static final double DEFAULT_CONVERGENCE_DELTA = 0;
  private static final double DEFAULT_DOC_TOPIC_SMOOTHING = 0.2;
  private static final double DEFAULT_TERM_TOPIC_SMOOTHING = 0.01;
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
    long seed = hasOption(RANDOM_SEED)
      ? Long.parseLong(getOption(RANDOM_SEED))
      : System.nanoTime() % 10000;
    float testFraction = hasOption(TEST_SET_FRACTION)
      ? Float.parseFloat(getOption(TEST_SET_FRACTION))
      : 0.0f;
    int numReduceTasks = Integer.parseInt(getOption(NUM_REDUCE_TASKS));
    Configuration conf=getConf();
    conf.set(NUM_TOPICS, String.valueOf(numTopics));
    conf.set(NUM_TERMS, String.valueOf(numTerms));
    conf.set(DOC_TOPIC_SMOOTHING, String.valueOf(alpha));
    conf.set(TERM_TOPIC_SMOOTHING, String.valueOf(eta));
    conf.set(RANDOM_SEED, String.valueOf(seed));
    conf.set(NUM_TRAIN_THREADS, String.valueOf(numTrainThreads));
    conf.set(NUM_UPDATE_THREADS, String.valueOf(numUpdateThreads));
    conf.set(MAX_ITERATIONS_PER_DOC, String.valueOf(maxItersPerDoc));
    conf.set(MODEL_WEIGHT, "1"); // TODO
    conf.set(TEST_SET_FRACTION, String.valueOf(testFraction));
    conf.set(NUM_REDUCE_TASKS,String.valueOf(numReduceTasks));
    Job infJob=prepareJob(conf,inputPath,topicModelOutputPath,docTopicOutputPath);
    infJob.submit();
    infJob.waitForCompletion(true);

    return 0;  //To change body of implemented methods use File | Settings | File Templates.
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

  public static Job prepareJob(Configuration conf, Path corpus, Path modelInput, Path output)
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
    LLDADriver.setModelPaths(job, modelInput);
    FileInputFormat.addInputPath(job, corpus);
    FileOutputFormat.setOutputPath(job, output);
    job.setJarByClass(LLDAInfDriver.class);
    //job.submit();
    return job;
  }


  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new LLDAInfDriver(), args);
  }
}
