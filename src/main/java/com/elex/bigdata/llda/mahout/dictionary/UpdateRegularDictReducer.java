package com.elex.bigdata.llda.mahout.dictionary;

import com.elex.bigdata.hashing.BDMD5;
import com.elex.bigdata.hashing.HashingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 6/17/14
 * Time: 3:41 PM
 * To change this template use File | Settings | File Templates.
 */
public class UpdateRegularDictReducer extends Reducer<Text,NullWritable,Text,IntWritable> {
  private RegularDictionray regularDictionray;
  private Integer dictId;
  private Path dictPath,dictSizePath,tmpDictPath;
  public void setup(Context context) throws IOException, InterruptedException {
    Configuration conf=context.getConfiguration();
    FileSystem fs=FileSystem.get(conf);
    try {
      regularDictionray=new RegularDictionray(conf.get(UpdateDictDriver.DICT_PATH),fs);
      regularDictionray.getDayDicts();
      dictId=regularDictionray.getDictSize();
    } catch (SQLException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (ClassNotFoundException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (HashingException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }
  public void reduce(Text key,Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
    try {
      regularDictionray.updateWord(BDMD5.getInstance().toMD5(key.toString()));
    } catch (HashingException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (SQLException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
  public void cleanup(Context context) throws IOException {
    try {
      regularDictionray.flushDict();
    } catch (SQLException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (ClassNotFoundException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
}
