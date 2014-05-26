package com.elex.bigdata.llda.mahout.data;

import org.apache.mahout.math.MultiLabelVectorWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.DoubleDoubleFunction;
import org.apache.mahout.math.function.Functions;
import org.apache.mahout.math.function.IntProcedure;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenIntDoubleHashMap;
import org.apache.mahout.math.map.OpenObjectIntHashMap;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/22/14
 * Time: 3:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class LabeledDocument {
  private Vector labels;
  private Vector urlCounts;
  public LabeledDocument(Vector labels,Vector urlCounts){
    this.labels=labels;
    this.urlCounts=urlCounts;
  }
  public Vector getLabels(){
    return labels;
  }
  public Vector getUrlCounts(){
    return urlCounts;
  }
  public void setLabels(Vector labels){
    this.labels=labels;
  }
  public void setUrlCounts(Vector urlCounts){
    this.urlCounts=urlCounts;
  }
  public static MultiLabelVectorWritable mergeDocs(List<MultiLabelVectorWritable> lDocs){
    Set<Integer> labelSet=new HashSet<Integer>();
    OpenIntDoubleHashMap urlCountMap=new OpenIntDoubleHashMap();

    for(int i=0;i<lDocs.size();i++){
      MultiLabelVectorWritable labelVectorWritable=lDocs.get(i);
      for(Integer label: labelVectorWritable.getLabels())
        labelSet.add(label);
      Vector tmpUrlCounts=lDocs.get(i).getVector();
      Iterator<Vector.Element> tmpUrlCountIter=tmpUrlCounts.iterateNonZero();
      while(tmpUrlCountIter.hasNext()){
        Vector.Element urlCount=tmpUrlCountIter.next();
        int termIndex=urlCount.index();
        urlCountMap.put(termIndex,urlCount.get()+urlCountMap.get(termIndex));
      }
    }
    Vector finalUrlCounts=new RandomAccessSparseVector(urlCountMap.size()*2);
    for(Integer url: urlCountMap.keys().elements()){
       finalUrlCounts.setQuick(url,urlCountMap.get(url));
    }
    int[] labels=new int[labelSet.size()];
    int i=0;
    for(Integer label: labelSet)
       labels[i++]=label;
    return new MultiLabelVectorWritable(finalUrlCounts,labels);
  }
}
