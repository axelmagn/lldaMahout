package com.elex.bigdata.llda.mahout.data.inputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/11/14
 * Time: 2:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class CombineTextInputFormat extends CombineFileInputFormat<LongWritable,Text> {
  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
    CombineFileSplit combineFileSplit = (CombineFileSplit) inputSplit;
    CombineFileRecordReader<LongWritable, Text> recordReader = new CombineFileRecordReader<LongWritable, Text>(combineFileSplit, taskAttemptContext, CombineTextRecordReader.class);
    try {
      recordReader.initialize(combineFileSplit, taskAttemptContext);
    } catch (InterruptedException e) {
      new RuntimeException("Error to initialize CombineSmallfileRecordReader.");
    }
    return recordReader;

  }
}
