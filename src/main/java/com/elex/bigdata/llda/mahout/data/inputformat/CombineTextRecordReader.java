package com.elex.bigdata.llda.mahout.data.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/11/14
 * Time: 2:39 PM
 * To change this template use File | Settings | File Templates.
 */
public class CombineTextRecordReader extends RecordReader<LongWritable,Text>{
  private CombineFileSplit combineFileSplit;
  private LineRecordReader lineRecordReader = new LineRecordReader();
  private Path[] paths;
  private int totalLength;
  private int currentIndex;
  private float currentProgress = 0;
  private LongWritable currentKey;
  private Text currentValue = new Text();

  public CombineTextRecordReader(CombineFileSplit combineFileSplit, TaskAttemptContext context, Integer index) throws IOException {
    super();
    this.combineFileSplit = combineFileSplit;
    this.currentIndex = index;
  }

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    this.combineFileSplit = (CombineFileSplit) inputSplit;
    // 处理CombineFileSplit中的一个小文件Block，因为使用LineRecordReader，需要构造一个FileSplit对象，然后才能够读取数据
    FileSplit fileSplit = new FileSplit(combineFileSplit.getPath(currentIndex), combineFileSplit.getOffset(currentIndex), combineFileSplit.getLength(currentIndex), combineFileSplit.getLocations());
    lineRecordReader.initialize(fileSplit, taskAttemptContext);

    this.paths = combineFileSplit.getPaths();
    totalLength = paths.length;
    taskAttemptContext.getConfiguration().set("map.input.file.name", combineFileSplit.getPath(currentIndex).getName());

  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (currentIndex >= 0 && currentIndex < totalLength) {
      return lineRecordReader.nextKeyValue();
    } else {
      return false;
    }
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    currentKey = lineRecordReader.getCurrentKey();
    return currentKey;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    currentValue= lineRecordReader.getCurrentValue();
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
    lineRecordReader.close();
  }
}
