package com.elex.bigdata.llda.mahout.est;

import com.elex.bigdata.llda.mahout.math.SparseRowDenseColumnMatrix;
import com.elex.bigdata.llda.mahout.math.SparseRowSparseColumnMatrix;
import com.elex.bigdata.llda.mahout.math.SparseRowSqSparseColumnMatrix;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.AbstractMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.Functions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/22/14
 * Time: 9:43 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestEstimator {
  private Pair<AbstractMatrix,Vector> pair=randomMatrix(20,100*1000, RandomUtils.getRandom(1234L));
  private Matrix topicTermCounts=pair.getFirst();
  private Vector topicSums=pair.getSecond();
  private double eta=0.02;
  private double alpha=0.3;
  private int[] topics=new int[]{0,1,2,3,4,7,8,10};
  @Test
  public void testTrain(){
     List<Integer> terms=new ArrayList<Integer>();
     List<Double> counts=new ArrayList<Double>();
     for(int i=0;i<10;i++)
     {
       terms.add(i);
       counts.add(new Double(i*10+1));
     }
     int[] labels=new int[]{0,2,4,8};
     Matrix matrix=new SparseRowSparseColumnMatrix(20,10);
     double[] termSums=new double[10];
     pTopicGivenTerm(terms,labels,matrix,termSums);
     normByTopicAndMultiByCount(counts,termSums,labels,matrix);
     System.out.println("hh");
  }

  @Test
  public void testInf(){
    List<Integer> terms=new ArrayList<Integer>();
    List<Double> counts=new ArrayList<Double>();
    for(int i=0;i<10;i++)
    {
      terms.add(i);
      counts.add(new Double(i*10+1));
    }
    int[] labels=new int[]{0,2,4,8};
    Vector result=inf(counts,terms,labels);
    System.out.println("hh");
    System.out.println(result.norm(1));
    StringBuilder builder=new StringBuilder();
    Iterator<Vector.Element> iter = result.iterateNonZero();
    while (iter.hasNext()) {
      Vector.Element e = iter.next();
      builder.append(e.index() + ":" + e.get() + ",");
    }
    System.out.println(builder.toString());
  }

  public Vector inf(List<Double> counts,List<Integer> terms, int[] labels) {
    Matrix docTopicTermDist = new SparseRowSqSparseColumnMatrix(20, 10);
    double[] termSums=new double[counts.size()];
    pTopicGivenTerm(terms, labels, docTopicTermDist,termSums);
    normByTopicAndMultiByCount(counts,termSums,labels,docTopicTermDist);
    Vector result = new DenseVector(20);
    double docTermCount=0.0;
    for(Double count: counts){
       docTermCount+=count;
    }
    for (int topic : topics) {
      result.set(topic, (docTopicTermDist.viewRow(topic).norm(1) + alpha) / (docTermCount + 100*1000 * alpha));
    }
    System.out.println(result.norm(1));
    result.assign(Functions.mult(1 / result.norm(1)));
    return result;
  }
  private void normByTopicAndMultiByCount(List<Double> counts, double[] termSums,int[] labels,Matrix perTopicSparseDistributions) {
    // then make sure that each of these is properly normalized by topic: sum_x(p(x|t,d)) = 1
    for(int topic: labels){
      Vector termDist=perTopicSparseDistributions.viewRow(topic);
      int i=0;
      Iterator<Vector.Element> iter=termDist.iterateNonZero();
      while(iter.hasNext()){
        Vector.Element e=iter.next();
        e.set(e.get()*counts.get(i)/termSums[i]);
        i++;
      }
    }
  }
  private void pTopicGivenTerm(List<Integer> terms, int[] topicLabels, Matrix termTopicDist,double[] termSum) {
    double Vbeta = 0.02 * 1000*1000;
    for (Integer topicIndex : topicLabels) {
      Vector termTopicRow = termTopicDist.viewRow(topicIndex);
      Vector topicTermRow = topicTermCounts.viewRow(topicIndex);
      double topicSum = topicSums.getQuick(topicIndex);
      double docTopicSum = 0.0;
      for (Integer termIndex : terms) {
        docTopicSum += topicTermRow.getQuick(termIndex);
      }
      for (int i=0;i<terms.size();i++) {
        int termIndex=terms.get(i);
        double topicTermCount = topicTermRow.getQuick(termIndex);
        double topicWeight = docTopicSum - topicTermCount;
        double termTopicLikelihood = (topicTermCount + eta) * (topicWeight + alpha) / (topicSum + Vbeta);
        termTopicRow.setQuick(termIndex, termTopicLikelihood);
        termSum[i]+=termTopicLikelihood;
      }
      //termTopicDist.assignRow(topicIndex, termTopicRow);
    }
  }
  private static Pair<AbstractMatrix, Vector> randomMatrix(int topicNum, int numTerms, Random random) {
    AbstractMatrix topicTermCounts = new SparseRowDenseColumnMatrix(topicNum, numTerms);
    Vector topicSums = new DenseVector(topicNum);
    if (random != null) {
      for (int topic=0;topic<topicNum;topic++) {
        for (int term = 0; term < numTerms; term++) {
          topicTermCounts.set(topic, term, random.nextDouble());
        }
      }
    }
    for (int topic=0;topic<topicNum;topic++) {
      topicSums.setQuick(topic, random == null ? 1.0 : topicTermCounts.viewRow(topic).norm(1));
    }
    //assert topicTermCounts.rowSize() > 100;
    return Pair.of(topicTermCounts, topicSums);
  }
}
