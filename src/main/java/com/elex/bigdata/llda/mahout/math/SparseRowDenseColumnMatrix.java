package com.elex.bigdata.llda.mahout.math;

import com.google.common.collect.AbstractIterator;
import org.apache.mahout.math.*;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/21/14
 * Time: 9:54 AM
 * To change this template use File | Settings | File Templates.
 */
public class SparseRowDenseColumnMatrix extends AbstractMatrix {
  private DenseVector[] rowVectors;
  public SparseRowDenseColumnMatrix(int rows,int columns){
     super(rows,columns);
     rowVectors =new DenseVector[rows];
     Arrays.fill(rowVectors,null);
  }
  public SparseRowDenseColumnMatrix(int rows,int columns,DenseVector[] vectors) throws Exception {
    super(rows,columns);
    this.rowVectors =new DenseVector[rows];
    Arrays.fill(this.rowVectors,null);
    for(int i=0;i<vectors.length;i++){
       if(vectors[i]!=null){
          if(vectors[i].size()>columns)
            throw new Exception(" vector size greater than matrix columns");
          this.rowVectors[i]=vectors[i];
       }
    }
  }
  @Override
  public Matrix clone() {
    SparseRowDenseColumnMatrix clone = (SparseRowDenseColumnMatrix) super.clone();
    clone.rowVectors = rowVectors.clone();
    return clone;
  }

  @Override
  public Iterator<MatrixSlice> iterator() {
    return new AbstractIterator<MatrixSlice>() {
      private int slice=-1;
      @Override
      protected MatrixSlice computeNext() {
        slice++;
        if (slice >= rowVectors.length) {
          return endOfData();
        }
        while(rowVectors[slice]==null)
        {
          slice++;
          if(slice>=rowVectors.length)
            return endOfData();
        }
        Vector row = rowVectors[slice];
        return new MatrixSlice(row, slice);
      }
    };
  }

  @Override
  public double getQuick(int row, int column) {
    if(rowVectors[row]==null)
      return 0.0;
    return rowVectors[row].getQuick(column);
  }

  @Override
  public Matrix like() {
    return new SparseRowDenseColumnMatrix(rowSize(), columnSize());
  }

  @Override
  public Matrix like(int rows, int columns) {
    return new SparseRowDenseColumnMatrix(rows, columns);
  }

  @Override
  public void setQuick(int row, int column, double value) {
    if(rowVectors[row]==null){
      rowVectors[row]=new DenseVector(columnSize());
    }
    rowVectors[row].setQuick(column, value);
  }

  @Override
  public int[] getNumNondefaultElements() {
    int[] result = new int[2];
    int rowSize=0;
    for(int i=0;i<rowVectors.length;i++){
      if(rowVectors[i]!=null)
        rowSize++;
    }
    result[ROW] = rowSize;
    for (Vector vectorEntry : rowVectors) {
      if(vectorEntry==null)
        continue;
      result[COL] = Math.max(result[COL], vectorEntry
        .getNumNondefaultElements());
    }
    return result;
  }

  @Override
  public Matrix viewPart(int[] offset, int[] size) {
    if (offset[ROW] < 0) {
      throw new IndexException(offset[ROW], rowSize());
    }
    if (offset[ROW] + size[ROW] > rowSize()) {
      throw new IndexException(offset[ROW] + size[ROW], rowSize());
    }
    if (offset[COL] < 0) {
      throw new IndexException(offset[COL], columnSize());
    }
    if (offset[COL] + size[COL] > columnSize()) {
      throw new IndexException(offset[COL] + size[COL], columnSize());
    }
    return new MatrixView(this, offset, size);
  }

  @Override
  public Matrix assignColumn(int column, Vector other) {
    if (rowSize() != other.size()) {
      throw new CardinalityException(rowSize(), other.size());
    }
    if (column < 0 || column >= columnSize()) {
      throw new IndexException(column, columnSize());
    }
    for (int row = 0; row < rowSize(); row++) {
      double val = other.getQuick(row);
      if (val != 0.0) {
        if(rowVectors[row]==null){
          rowVectors[row]=new DenseVector(columnSize());
        }
        rowVectors[row].setQuick(column, val);
      }
    }
    return this;
  }

  @Override
  public Matrix assignRow(int row, Vector other) {
    if (columnSize() != other.size()) {
      throw new CardinalityException(columnSize(), other.size());
    }
    if (row < 0 || row >= rowSize()) {
      throw new IndexException(row, rowSize());
    }
    if(!(other instanceof DenseVector)){
      try {
        throw new Exception(" vector should be denseVector");
      } catch (Exception e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
    rowVectors[row]=(DenseVector)other;
    return this;
  }

  @Override
  public Vector viewRow(int row) {
    if (row < 0 || row >= rowSize()) {
      throw new IndexException(row, rowSize());
    }
    if(rowVectors[row]==null){
      rowVectors[row]=new DenseVector(columnSize());
    }
    return rowVectors[row];
  }
}
