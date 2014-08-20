package com.elex.bigdata.llda.mahout.data.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.mahout.math.MultiLabelVectorWritable;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/20/14
 * Time: 5:04 PM
 * To change this template use File | Settings | File Templates.
 */
public class CombineSqRecordReader extends RecordReader<Text,MultiLabelVectorWritable> {
  private CombineFileSplit combineFileSplit;
  private SequenceFileRecordReader<Text,MultiLabelVectorWritable> reader=new SequenceFileRecordReader<Text,MultiLabelVectorWritable>();
  private int currentIndex;
  private int totalLength;
  private Path[] paths;
  private Text currentKey;
  private float currentProgress = 0;
  private MultiLabelVectorWritable currentValue;
  public CombineSqRecordReader(CombineFileSplit combineFileSplit, TaskAttemptContext context, Integer index) throws IOException {
    super();
    this.combineFileSplit = combineFileSplit;
    this.currentIndex = index;
  }
  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    FileSplit fileSplit = new FileSplit(combineFileSplit.getPath(currentIndex), combineFileSplit.getOffset(currentIndex), combineFileSplit.getLength(currentIndex), combineFileSplit.getLocations());
    reader.initialize(fileSplit, taskAttemptContext);

    this.paths = combineFileSplit.getPaths();
    totalLength = paths.length;
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (currentIndex >= 0 && currentIndex < totalLength) {
      return reader.nextKeyValue();
    } else {
      return false;
    }
  }

  @Override
  public Text getCurrentKey() throws IOException, InterruptedException {
    currentKey = reader.getCurrentKey();
    return currentKey;
  }

  @Override
  public MultiLabelVectorWritable getCurrentValue() throws IOException, InterruptedException {
    currentValue= reader.getCurrentValue();
    return currentValue;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    if (currentIndex >= 0 && currentIndex < totalLength) {
      currentProgress = (float) currentIndex / totalLength;
      return currentProgress;
    }
    return currentProgress;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
