package com.elex.bigdata.llda.mahout.data.complementdocs;

import com.elex.bigdata.llda.mahout.data.LabeledDocument;
import com.elex.bigdata.llda.mahout.data.LabeledDocumentWritable;
import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver;
import com.elex.bigdata.llda.mahout.dictionary.UpdateDictDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.MultiLabelVectorWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 11:27 AM
 * To change this template use File | Settings | File Templates.
 */
public class ComplementLDocMapper extends Mapper<Text, MultiLabelVectorWritable, Text, MultiLabelVectorWritable> {
  private Set<String> uids = new HashSet<String>();
  private int termSize;

  public void setup(Context context) throws IOException {
    /*
       extract uid from uidFile
     */
    Configuration conf = context.getConfiguration();
    Path uidPath = new Path(conf.get(GenerateLDocDriver.UID_PATH));
    SequenceFile.Reader uidReader = new SequenceFile.Reader(FileSystem.get(conf), uidPath, conf);
    Text uid = new Text();
    NullWritable nullWritable = NullWritable.get();
    while (uidReader.next(uid, nullWritable)) {
      uids.add(uid.toString());
    }
    uidReader.close();

    Path dictSizePath = new Path(conf.get(UpdateDictDriver.DICT_SIZE_PATH));
    SequenceFile.Reader dictSizeReader = new SequenceFile.Reader(FileSystem.get(conf), dictSizePath, conf);
    IntWritable dictSizeWritable = new IntWritable();
    dictSizeReader.next(dictSizeWritable, nullWritable);
    termSize = dictSizeWritable.get();
  }

  public void map(Text key, MultiLabelVectorWritable value, Context context) throws IOException, InterruptedException {
    if (uids.contains(key.toString())) {
      Vector urlCounts=value.getVector();
      if(urlCounts.size()<termSize){
        Vector tmpUrlCounts=new RandomAccessSparseVector(termSize);
        Iterator<Vector.Element> urlCountIter=urlCounts.iterateNonZero();
        while(urlCountIter.hasNext()){
          Vector.Element e=urlCountIter.next();
          tmpUrlCounts.set(e.index(),e.get());
        }
        value.setVector(tmpUrlCounts);
      }
      context.write(key,value);
    }
  }
}
