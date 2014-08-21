package com.elex.bigdata.llda.mahout.math;

import com.google.common.collect.AbstractIterator;
import org.apache.mahout.math.AbstractVector;
import org.apache.mahout.math.CardinalityException;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;

import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/21/14
 * Time: 2:33 PM
 * To change this template use File | Settings | File Templates.
 */
public class SequencialSparseVector extends AbstractVector {
  private double[] values;
  private int[] indexs;
  private int arrayIndex=0;
  public SequencialSparseVector(int size){
    super(size);
    values=new double[size];
    indexs=new int[size];
  }
  @Override
  protected Matrix matrixLike(int rows, int columns) {
    return new SparseRowDenseColumnMatrix(rows,columns);  //To change body of implemented methods use File | Settings | File Templates.
  }
  @Override
  public Vector assign(Vector other){
    if (size() != other.size()) {
      throw new CardinalityException(size(), other.size());
    }
    arrayIndex=0;
    for(Element e: other){
      setQuick(e.index(), e.get());
    }
    return this;
  }

  @Override
  public boolean isDense() {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean isSequentialAccess() {
    return true;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Iterator<Element> iterator() {
    return new AllIterator();
  }

  @Override
  public Iterator<Element> iterateNonZero() {
    return new AllIterator();  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public double getQuick(int index) {
    for(int i=0;i<size();i++){
          if(indexs[i]==index)
          return values[i];
    }
    return 0.0;
  }

  @Override
  public Vector like() {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void setQuick(int index, double value) {
    if(arrayIndex>=size())
      return;
    values[arrayIndex]=value;
    indexs[arrayIndex]=index;
    arrayIndex++;
  }
  @Override
  public Vector assign(double value) {
    arrayIndex=0;
    for (int i = 0; i < size(); i++) {
      setQuick(i, value);
    }
    return this;
  }
  @Override
  public Vector assign(double[] values) {
    if (size() != values.length) {
      throw new CardinalityException(size(), values.length);
    }
    arrayIndex=0;
    for (int i = 0; i < size(); i++) {
      setQuick(i, values[i]);
    }
    return this;
  }

  @Override
  public void set(int index,double value){
    setQuick(index,value);
  }

  @Override
  public int getNumNondefaultElements() {
    return arrayIndex;
  }

  private final class AllIterator extends AbstractIterator<Element> {

    private final SqSparseElement element = new SqSparseElement();

    private AllIterator() {
      element.index = -1;
    }

    @Override
    protected Element computeNext() {
      if (element.index + 1 < arrayIndex) {
        element.index++;
        return element;
      } else {
        return endOfData();
      }
    }

  }

  private final class SqSparseElement implements Element{
    private int index;
    @Override
    public double get() {
      return values[index];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int index() {
      return indexs[index];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void set(double value) {
      values[index]=value;
    }
  }
}
