package com.elex.bigdata.llda.mahout.mapreduce.etl;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/28/14
 * Time: 11:18 AM
 * To change this template use File | Settings | File Templates.
 */
public class ResultEtlMapper extends Mapper<Object,Text,Text,Text> {
  private static int CATEGORY_COUNT=4;
  public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
    String[] results=value.toString().split("\t");
    String[] probs=results[2].split(",");
    List<Integer> probabilities=new ArrayList<Integer>();
    for(int i=0;i<4;i++)
      probabilities.add((int)(Double.parseDouble(probs[i].split(":")[1])*100));
    StringBuilder probBuilder=new StringBuilder();
    String[] probStrs=new String[]{"a","b","c","d","z"};
    /*
    int probLeft=100;
    for(int i=0;i<CATEGORY_COUNT;i++){
      probBuilder.append(probStrs[i]+"=");
      int prob=probabilities.get(i);
      probLeft-=prob;
      if(prob<16)
        probBuilder.append("0");
      probBuilder.append(Integer.toHexString(prob));
      probBuilder.append(";");
    }
    probBuilder.append(probStrs[CATEGORY_COUNT]+"=");
    if(probLeft<16)
      probBuilder.append("0");
    probBuilder.append(Integer.toHexString(probLeft));
    context.write(new Text(results[0]),new Text(probBuilder.toString()));
    */
    // output the category (in a,b,c,d) which has the biggest probality
    int maxProbIndex=0;
    double maxProb=probabilities.get(0);
    for(int i=1;i<4;i++)
      if(probabilities.get(i)>maxProb){
        maxProbIndex=i;
        maxProb=probabilities.get(i);
      }
    if(maxProb>3)
      context.write(new Text(results[0]),new Text(probStrs[maxProbIndex]));
    else
      context.write(new Text(results[0]),new Text(probStrs[4]));
  }
}
