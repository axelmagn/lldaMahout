package com.elex.bigdata.llda.mahout.util;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/19/14
 * Time: 11:00 AM
 * To change this template use File | Settings | File Templates.
 */
public class MathUtil {
  /*

   */
  public static int getMax(int[] topics){
    int max=0;
    for(int i=0;i<topics.length;i++){
      if(topics[i]>max){
        max=topics[i];
      }
    }
    return max;
  }
}
