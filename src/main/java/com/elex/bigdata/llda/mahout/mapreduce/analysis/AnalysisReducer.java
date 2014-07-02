package com.elex.bigdata.llda.mahout.mapreduce.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/2/14
 * Time: 3:51 PM
 * To change this template use File | Settings | File Templates.
 */
public class AnalysisReducer extends Reducer<Text,IntWritable,Text,Text> {
  private int[] thresholds=new int[]{2,4,8,16,32,64,128,256,512,1024,2048,4096,9192,18384};
  private int[] counts=new int[thresholds.length+1];
  private int allCount=0;
  public void setup(Context context){
    for(int i=0;i<counts.length;i++){
      counts[i]=0;
    }
  }
  public void reduce(Text key,Iterable<IntWritable> values,Context context){
    int count=0;
    Iterator<IntWritable> iterator=values.iterator();
    while(iterator.hasNext()){
      count+=iterator.next().get();
    }
    int i;
    for(i=0;i<thresholds.length;i++){
      if(count<=thresholds[i])
        break;
    }
    counts[i]+=1;
    allCount+=1;
  }

  public void cleanup(Context context) throws IOException, InterruptedException {
    Text key=new Text("0~"+thresholds[0]);
    Text value=new Text(String.valueOf(counts[0]));
    context.write(key,value);
    for(int i=1;i<thresholds.length;i++){
      key=new Text(thresholds[i-1]+"~"+thresholds[i]);
      value=new Text(String.valueOf(counts[i]));
      context.write(key,value);
    }
    context.write(new Text(thresholds[thresholds.length-1]+"~"),new Text(String.valueOf(counts[counts.length-1])));
    context.write(new Text("all"),new Text(String.valueOf(allCount)));
  }
}
