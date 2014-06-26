package com.elex.bigdata.llda.mahout.data.mergedocs;

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
 * Time: 10:27 AM
 * To change this template use File | Settings | File Templates.
 */
public class MergeLDocMapper extends Mapper<Text,MultiLabelVectorWritable,Text,MultiLabelVectorWritable> {
  private Set<String> uids = new HashSet<String>();
  private boolean filtering;
  public void setup(Context context) throws IOException {
    /*
       extract uid from uidFile
     */
    filtering=false;
    Configuration conf = context.getConfiguration();
    initFiltering(conf);
  }

  private void initFiltering(Configuration conf) throws IOException {
    String uid_file=conf.get(GenerateLDocDriver.UID_PATH);
    if(uid_file==null)
      return;
    Path uidPath = new Path(uid_file);
    SequenceFile.Reader uidReader = new SequenceFile.Reader(FileSystem.get(conf), uidPath, conf);
    Text uid = new Text();
    NullWritable nullWritable = NullWritable.get();
    while (uidReader.next(uid, nullWritable)) {
      uids.add(uid.toString());
    }
    uidReader.close();
    System.out.println("filtering is "+filtering);
  }
  public void map(Text key,MultiLabelVectorWritable value,Context context) throws IOException, InterruptedException {
     /*
        create a labeledDocument with size of dictSize according to value
     */
    if(!filtering||uids.contains(key.toString()))
      context.write(key,value);
  }
}
