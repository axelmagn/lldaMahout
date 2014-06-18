package com.elex.bigdata.llda.mahout.dictionary;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/13/14
 * Time: 10:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class UpdateDictMapper extends Mapper<Object,Text,Text,NullWritable> {
  private static final Logger log = LoggerFactory.getLogger(UpdateDictMapper.class);
  public void map(Object key,Text value ,Context context) throws IOException, InterruptedException {
     String[] uidUrlCount=value.toString().split("\t");
     if(uidUrlCount.length<2)
     {
       log.warn("wrong uidUrlCount "+value.toString());
       return;
     }
     String url=uidUrlCount[1];
     context.write(new Text(url), NullWritable.get());
  }
}
