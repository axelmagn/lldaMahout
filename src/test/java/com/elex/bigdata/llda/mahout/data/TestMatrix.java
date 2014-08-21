package com.elex.bigdata.llda.mahout.data;

import com.elex.bigdata.llda.mahout.math.SequencialSparseVector;
import com.elex.bigdata.llda.mahout.math.SparseRowDenseColumnMatrix;
import org.apache.mahout.math.*;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.function.DoubleFunction;
import org.apache.mahout.math.function.Functions;
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
      matrixSlice.set(e.index(), e.get());
    }
    matrix.assignRow(row,matrixSlice);
    /*
    for(Element e: matrixSlice){
      System.out.print(e.index()+":"+e.get()+",");
    }
    */
  }

  @Test
  public void testVector(){
    Vector vector1=new DenseVector(1000*1000*2);
    vector1.assign(1.0);
    Vector vector2=new DenseVector(1000*1000*2);
    vector2.assign(2.0);
    long t1=System.nanoTime();
    vector1.assign(vector2, Functions.PLUS);
    long t2=System.nanoTime();
    System.out.println(t2-t1);


    Vector vector3=new RandomAccessSparseVector(1000*1000);
    vector3.assign(1.0);
    Vector vector4=new RandomAccessSparseVector(1000*1000*2);
    vector4.assign(1.0);
    t1=System.nanoTime();
    for(Element e : vector1){
      vector4.setQuick(e.index(),vector4.getQuick(e.index())+e.get());
    }
    t2=System.nanoTime();
    System.out.println(t2-t1);
    t1=System.nanoTime();
    for(Element e : vector1){
      vector2.setQuick(e.index(),vector2.getQuick(e.index())+e.get());
    }
    t2=System.nanoTime();
    System.out.println(t2-t1);


  }
  @Test
  public void testSparseRowDenseColumnMatrix(){
    long t1=System.nanoTime();

    SparseRowDenseColumnMatrix matrix=new SparseRowDenseColumnMatrix(148,1000000);
    for(int i=0;i<148;i=i+2){
       matrix.viewRow(i).assign(i+1);
    }

    long t2=System.nanoTime();
    System.out.println((t2-t1)/1000);
    for(MatrixSlice matrixSlice: matrix){
      System.out.println(matrixSlice.index());

    }
    System.out.println(matrix.getNumNondefaultElements()[0]+","+matrix.getNumNondefaultElements()[1]);
    matrix.assign(2.0);
    System.out.println(matrix.get(0, 1));
    matrix.set(3,4,5);
    System.out.println(matrix.get(3,4));

  }
  @Test
  public void testSqSparseVector(){
    SequencialSparseVector sequencialSparseVector=new SequencialSparseVector(1000);
    for(int i=0;i<sequencialSparseVector.size();i++){
      sequencialSparseVector.setQuick(1000-i,i);
    }
    for(Element e: sequencialSparseVector){
      System.out.print(e.index()+":"+e.get()+",");
    }
    System.out.println();
    sequencialSparseVector.setQuick(10000,1000);
    sequencialSparseVector.setQuick(10001,1001);
    sequencialSparseVector.assign(1.0);
    System.out.println(sequencialSparseVector.getNumNondefaultElements());
    System.out.println(sequencialSparseVector.getQuick(677));
  }
}
