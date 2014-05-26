package com.elex.bigdata.llda.mahout.data.mergedocs;

import com.elex.bigdata.llda.mahout.data.LabeledDocument;
import com.elex.bigdata.llda.mahout.data.LabeledDocumentWritable;
import com.elex.bigdata.llda.mahout.dictionary.UpdateDictDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.MultiLabelVectorWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 10:27 AM
 * To change this template use File | Settings | File Templates.
 */
public class MergeLDocMapper extends Mapper<Text,MultiLabelVectorWritable,Text,MultiLabelVectorWritable> {

  public void map(Text key,MultiLabelVectorWritable value,Context context) throws IOException, InterruptedException {
     /*
        create a labeledDocument with size of dictSize according to value
     */
    context.write(key,value);
  }
}
