package com.elex.bigdata.llda.mahout.data.mergedocs;

import com.elex.bigdata.llda.mahout.data.LabeledDocument;
import com.elex.bigdata.llda.mahout.data.LabeledDocumentWritable;
import com.elex.bigdata.llda.mahout.dictionary.UpdateDictDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 10:27 AM
 * To change this template use File | Settings | File Templates.
 */
public class MergeLDocMapper extends Mapper<Text,LabeledDocumentWritable,Text,LabeledDocumentWritable> {
  private int termSize=0;
  public void setup(Context context) throws IOException {
    /*
       get dictSize
     */
    Configuration conf=context.getConfiguration();
    Path dictSizePath=new Path(conf.get(UpdateDictDriver.DICT_SIZE_PATH));
    SequenceFile.Reader reader=new SequenceFile.Reader(FileSystem.get(conf),dictSizePath,conf);
    IntWritable dictSizeWritable=new IntWritable();
    NullWritable nullWritable=NullWritable.get();
    reader.next(dictSizeWritable,nullWritable);
    termSize=dictSizeWritable.get();

  }

  public void map(Text key,LabeledDocumentWritable value,Context context) throws IOException, InterruptedException {
     /*
        create a labeledDocument with size of dictSize according to value
     */
    LabeledDocument labeledDocument=value.get();
    Vector urlCounts=labeledDocument.getUrlCounts();
    if(urlCounts.size()<termSize){
      Vector tmpUrlCounts=new RandomAccessSparseVector(termSize);
      Iterator<Vector.Element> urlCountIter=urlCounts.iterateNonZero();
      while(urlCountIter.hasNext()){
        Vector.Element e=urlCountIter.next();
        tmpUrlCounts.set(e.index(),e.get());
      }
      labeledDocument.setUrlCounts(tmpUrlCounts);
      context.write(key,new LabeledDocumentWritable(labeledDocument));
    }else
      context.write(key,value);
  }
}
