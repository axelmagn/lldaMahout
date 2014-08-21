package com.elex.bigdata.llda.mahout.mapreduce.est;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.Functions;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/20/14
 * Time: 4:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class VectorSumReducer
  extends Reducer<WritableComparable<?>, VectorWritable, WritableComparable<?>, VectorWritable> {

  @Override
  protected void reduce(WritableComparable<?> key, Iterable<VectorWritable> values, Context ctx)
    throws IOException, InterruptedException {
    Vector vector = null;
    for (VectorWritable v : values) {
      if (vector == null) {
        vector = v.get();
      } else {
        long t1=System.nanoTime();
        vector.assign(v.get(), Functions.PLUS);
        long t2=System.nanoTime();
        System.out.println("plus within vectors use "+(t2-t1)/1000 + " us");
      }
    }
    ctx.write(key, new VectorWritable(vector));
  }
}