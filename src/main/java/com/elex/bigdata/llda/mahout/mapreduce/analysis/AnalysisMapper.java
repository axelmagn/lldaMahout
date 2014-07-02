package com.elex.bigdata.llda.mahout.mapreduce.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.MultiLabelVectorWritable;
import org.apache.mahout.math.Vector;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/2/14
 * Time: 3:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class AnalysisMapper extends Mapper<Text,MultiLabelVectorWritable,Text,IntWritable> {
  public void map(Text key,MultiLabelVectorWritable value,Context context) throws IOException, InterruptedException {
     Vector vector=value.getVector();
     Iterator<Vector.Element> iter=vector.iterateNonZero();
     int count=0;
     while(iter.hasNext()){
       Vector.Element e=iter.next();
       count+=e.get();
     }
     context.write(key,new IntWritable(count));
  }
}
