package com.elex.bigdata.llda.mahout.data;

import org.apache.mahout.math.*;
import org.apache.mahout.math.Vector.Element;
import org.junit.Test;

import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/20/14
 * Time: 9:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestMatrix {
  @Test
  public void testSparseMatrix(){
    Matrix matrix=new SparseMatrix(10,10);
    for(int i=0;i<10;i=i+2){
       Vector vector=new DenseVector(10);
       vector.assign((i+1.0));
       setValue(i,vector,matrix);
    }
    Iterator<MatrixSlice> iter=matrix.iterator();
    while(iter.hasNext()){
      MatrixSlice matrixSlice=iter.next();
      System.out.println(matrixSlice.index());
      for(Element e: matrixSlice.vector()){
        System.out.print(e.index()+":"+e.get()+",");
      }
      System.out.println();
    }
  }
  private void setValue(int row,Vector vector,Matrix matrix){
    Iterator<Element> iter=vector.iterateNonZero();
    Vector matrixSlice=matrix.viewRow(row);
    while(iter.hasNext()){
      Element e=iter.next();
      matrixSlice.set(e.index(),e.get());
    }
    matrix.assignRow(row,matrixSlice);
    /*
    for(Element e: matrixSlice){
      System.out.print(e.index()+":"+e.get()+",");
    }
    */
  }
}
