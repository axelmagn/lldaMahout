package com.elex.bigdata.llda.mahout.dictionary;

import com.elex.bigdata.hashing.BDMD5;
import com.elex.bigdata.hashing.HashingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 6/24/14
 * Time: 10:34 AM
 * To change this template use File | Settings | File Templates.
 */
public class Dictionary {
  /*
    fields:
      word_id_map : map of word:id
      dictSize : size of word
      fs : fileSystem
      dictRootPath: dict rootPath in hdfs
      conf: Configuration of hdfs
    functions:
      constructor(dictRootPath,fs);
      constructor();
      loadDict(): load dict from hdfs;use sequenceFile
      update(word):
          checkAndAdd word;
      contains(word):
          check if contained;
      getId(word):
          return word_id_map.get(word)
      flushDict():flush dict;use sequenceFile

   */
  private Logger log=Logger.getLogger(Dictionary.class);
  private Map<String, Integer> word_id_map;
  private AtomicInteger dictSize;
  private FileSystem fs;
  private Configuration conf;
  private Path dictRootPath;

  private static String DICT = "dict";
  private static String TMP_DICT = "tmpDict";
  public static String DICT_SIZE = "dictSize";

  public Dictionary() {
    dictSize = new AtomicInteger(0);
    word_id_map = new HashMap<String, Integer>();
  }

  public Dictionary(String dictRootPath, FileSystem fs, Configuration conf) throws IOException, HashingException {
    this.fs = fs;
    this.conf = conf;
    this.dictRootPath = new Path(dictRootPath);
    word_id_map = new HashMap<String, Integer>();
    dictSize = new AtomicInteger(0);
    loadDict();
  }

  private void loadDict() throws IOException, HashingException {
    if (!fs.exists(dictRootPath))
      fs.mkdirs(dictRootPath);
    Path dictPath = new Path(dictRootPath, DICT);
    Path dictSizePath = new Path(dictRootPath, DICT_SIZE);
    if (fs.exists(dictSizePath)) {
      SequenceFile.Reader dictSizeReader = new SequenceFile.Reader(fs, dictSizePath, conf);
      IntWritable dictSizeWritable = new IntWritable();
      dictSizeReader.next(dictSizeWritable, NullWritable.get());
      dictSize = new AtomicInteger(dictSizeWritable.get());
    }

    if (fs.exists(dictPath)) {
      SequenceFile.Reader dictReader = new SequenceFile.Reader(fs, dictPath, conf);
      Text word = new Text();
      IntWritable wordId = new IntWritable();
      while (dictReader.next(word, wordId)) {
        word_id_map.put(word.toString(), wordId.get());
      }
      if (dictSize.intValue() != word_id_map.size()) {
        System.out.println("dictSize not equal word_id_map.size ");
        dictSize = new AtomicInteger(word_id_map.size());
      }
    }
    System.out.println("load dict. dict size is "+dictSize.intValue());
  }

  public void update(String word) {
    if (word_id_map.containsKey(word))
      return;
    word_id_map.put(word, dictSize.getAndIncrement());
  }

  public boolean contains(String word) {
    if (word_id_map.containsKey(word))
      return true;
    return false;
  }

  public Integer getId(String word) {
    return word_id_map.get(word);
  }

  public static int getNumTerms(Configuration conf,Path dictRootPath) throws IOException {
    Path dictSizePath=new Path(dictRootPath, DICT_SIZE);
    FileSystem fs=FileSystem.get(conf);
    if(fs.exists(dictSizePath)){
    SequenceFile.Reader reader=new SequenceFile.Reader(fs, dictSizePath, conf);
    IntWritable dictSize=new IntWritable();
    reader.next(dictSize, NullWritable.get());
    return dictSize.get();
    }else{
      try {
        Dictionary dict=new Dictionary(dictRootPath.toString(),fs,conf);
        return dict.dictSize.intValue();
      } catch (HashingException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
    return -1;
  }


  public void flushDict() throws IOException {
    if(!fs.exists(dictRootPath)){
       log.warn(dictRootPath.toString()+" not exists");
       fs.mkdirs(dictRootPath);
    }
    Path tmpDictPath = new Path(dictRootPath, TMP_DICT);
    Path dictPath = new Path(dictRootPath, DICT);
    Path dictSizePath = new Path(dictRootPath, DICT_SIZE);
    SequenceFile.Writer dictWriter = SequenceFile.createWriter(fs, conf, tmpDictPath, Text.class, IntWritable.class);
    for (Map.Entry<String, Integer> entry : word_id_map.entrySet()) {
      dictWriter.append(new Text(entry.getKey()), new IntWritable(entry.getValue()));
    }
    dictWriter.hflush();
    dictWriter.close();
    fs.rename(tmpDictPath, dictPath);
    log.info("dictSize is "+dictSize.intValue());
    SequenceFile.Writer dictSizeWriter = SequenceFile.createWriter(fs, conf, dictSizePath, IntWritable.class, NullWritable.class);
    dictSizeWriter.append(new IntWritable(dictSize.intValue()), NullWritable.get());
    dictSizeWriter.hflush();
    dictSizeWriter.close();
  }

}
