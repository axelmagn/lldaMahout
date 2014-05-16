package com.elex.bigdata.llda.mahout.data;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.DoubleDoubleFunction;
import org.apache.mahout.math.function.Functions;
import org.apache.mahout.math.map.OpenObjectIntHashMap;

import java.util.Iterator;
import java.util.List;

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
  public static LabeledDocument mergeDocs(List<LabeledDocument> lDocs){
    Vector finalLabels=new RandomAccessSparseVector(lDocs.get(0).getLabels());
    Vector finalUrlCounts=new RandomAccessSparseVector(lDocs.get(0).getUrlCounts());
    for(int i=1;i<lDocs.size();i++){
      Vector tmpLabels=lDocs.get(i).getLabels();
      Iterator<Vector.Element> tmpLabelIter=tmpLabels.iterateNonZero();
      while(tmpLabelIter.hasNext()){
        Vector.Element e=tmpLabelIter.next();
        finalLabels.set(e.index(),e.get());
      }
      Vector tmpUrlCounts=lDocs.get(i).getUrlCounts();
      finalUrlCounts.assign(tmpUrlCounts, Functions.PLUS);
    }
    return new LabeledDocument(finalLabels,finalUrlCounts);
  }
}
