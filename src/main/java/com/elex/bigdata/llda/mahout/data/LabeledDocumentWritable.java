package com.elex.bigdata.llda.mahout.data;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.map.OpenObjectIntHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/22/14
 * Time: 4:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class LabeledDocumentWritable implements Writable {
  private LabeledDocument labeledDocument;
  public LabeledDocumentWritable(){

  }
  public LabeledDocumentWritable(LabeledDocument labeledDocument){
     this.labeledDocument=labeledDocument;
  }
  @Override
  public void write(DataOutput dataOutput) throws IOException {
    VectorWritable labelsWritable =new VectorWritable(labeledDocument.getLabels());
    VectorWritable urlCountsWritable =new VectorWritable(labeledDocument.getUrlCounts());
    labelsWritable.write(dataOutput);
    urlCountsWritable.write(dataOutput);

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    Vector labels=VectorWritable.readVector(dataInput);
    Vector urlCounts=VectorWritable.readVector(dataInput);
    labeledDocument=new LabeledDocument(labels,urlCounts);
  }

  public LabeledDocument get(){
    return labeledDocument;
  }
}
