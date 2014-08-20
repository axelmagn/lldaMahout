package com.elex.bigdata.llda.mahout.data.inputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.mahout.math.MultiLabelVectorWritable;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/20/14
 * Time: 5:17 PM
 * To change this template use File | Settings | File Templates.
 */
public class CombineSqInputFormat extends CombineFileInputFormat<Text,MultiLabelVectorWritable> {
  @Override
  public RecordReader<Text, MultiLabelVectorWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
    CombineFileSplit combineFileSplit = (CombineFileSplit) inputSplit;
    CombineFileRecordReader<Text, MultiLabelVectorWritable> recordReader = new CombineFileRecordReader<Text, MultiLabelVectorWritable>(combineFileSplit, taskAttemptContext, CombineSqRecordReader.class);
    try {
      recordReader.initialize(combineFileSplit, taskAttemptContext);
    } catch (InterruptedException e) {
      new RuntimeException("Error to initialize CombineSmallfileRecordReader.");
    }
    return recordReader;
  }
}
