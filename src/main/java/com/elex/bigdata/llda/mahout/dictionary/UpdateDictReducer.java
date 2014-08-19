package com.elex.bigdata.llda.mahout.dictionary;

import com.elex.bigdata.hashing.BDMD5;
import com.elex.bigdata.hashing.HashingException;
import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver;
import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocReducer;
import com.elex.bigdata.llda.mahout.util.PrefixTrie;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.Pair;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/13/14
 * Time: 10:48 PM
 * To change this template use File | Settings | File Templates.
 */
public class UpdateDictReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  /*
     fields:
         dict: Dictionary
         word_count_threshold:
   */
  private Dictionary dict;
  private BDMD5 bdmd5;
  private int word_count_threshold, word_count_upper_threshold;
  private Map<String, String> url_category_map = new HashMap<String, String>();
  PrefixTrie prefixTrie = new PrefixTrie();


  public void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    Pair<Map<String,String>,Map<String,Integer>> pair=GenerateLDocReducer.loadUrlTopics(conf,prefixTrie);
    url_category_map=pair.getFirst();
    word_count_threshold = Integer.parseInt(conf.get(UpdateDictDriver.COUNT_THRESHOLD));
    System.out.println("word count lower boundary is " + word_count_threshold);
    word_count_upper_threshold = Integer.parseInt(conf.get(UpdateDictDriver.COUNT_UPPER_THRESHOLD));
    System.out.println("word count upper boundary is " + word_count_upper_threshold);
    String dictRoot = conf.get(UpdateDictDriver.DICT_ROOT);
    System.out.println("dict Root is " + dictRoot);
    FileSystem fs=FileSystem.get(conf);
    try {
      dict = new Dictionary(dictRoot, fs, conf);
      bdmd5 = BDMD5.getInstance();
    } catch (HashingException e) {
      e.printStackTrace();
    }
  }

  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    int wordCount = 0;
    String word = key.toString();
    boolean shouldWrite = false;
    if (url_category_map.containsKey(key.toString()) || prefixTrie.prefixSearch(key.toString()) != -1) {
      shouldWrite = true;
    } else if (word.contains("shop") || word.contains("games") || word.contains("amazon")) {
      shouldWrite = true;
    } else {
      for (IntWritable countWritable : values) {
        wordCount += countWritable.get();
        if (wordCount >= word_count_upper_threshold) {
          break;
        }
      }
      if (wordCount >= word_count_threshold && wordCount < word_count_upper_threshold)
        shouldWrite = true;
    }
    if (shouldWrite) {
      try {
        dict.update(bdmd5.toMD5(word).substring(UpdateDictDriver.MD5_START_INDEX, UpdateDictDriver.MD5_END_INDEX));
      } catch (HashingException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
  }

  public void cleanup(Context context) throws IOException {
    dict.flushDict();
  }
}
