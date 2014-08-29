package com.elex.bigdata.llda.mahout.mapreduce.analysis;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.AbstractJob;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/29/14
 * Time: 4:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class LappedWordDriver extends AbstractJob{
  @Override
  public int run(String[] args) throws Exception {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  private static  class LappedWordMapper extends Mapper<Object,Text,Text,NullWritable> {

  }
}
