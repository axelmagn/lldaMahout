package com.elex.bigdata.llda.mahout.data.transferUid;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/3/14
 * Time: 4:10 PM
 * To change this template use File | Settings | File Templates.
 */
public class TransNtUidReducer extends Reducer<Text,Text,Text,Text> {
  public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
    Set<String> nations=new HashSet<String>();
    for(Text value: values){
      nations.add(value.toString());
    }
    for(String nation: nations){
      context.write(key,new Text(nation));
    };

  }
}
