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
  private String[] destCategories = new String[]{"jogos", "compras", "Friends", "Tourism"};
  Map<String, Integer> categoryIdMap = new HashMap<String, Integer>();
  PrefixTrie prefixTrie = new PrefixTrie();
  private Map<String, String> url_category_map = new HashMap<String, String>();

  public void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    for (int i = 0; i < destCategories.length; i++) {
      categoryIdMap.put(destCategories[i], i);
    }
    Path resourcesPath = new Path(conf.get(GenerateLDocDriver.RESOURCE_ROOT));
    Path urlCategoryPath = new Path(resourcesPath, GenerateLDocReducer.URL_TOPIC);
    BufferedReader urlCategoryReader = new BufferedReader(new InputStreamReader(fs.open(urlCategoryPath)));
    String line = "";
    while ((line = urlCategoryReader.readLine()) != null) {
      String[] categoryUrls = line.split(" ");
      if (categoryIdMap.containsKey(categoryUrls[0])) {
        int id = categoryIdMap.get(categoryUrls[0]);
        for (int i = 1; i < categoryUrls.length; i++)
          prefixTrie.insert(categoryUrls[i], id);
      } else {
        for (int i = 1; i < categoryUrls.length; i++) {
          url_category_map.put(categoryUrls[i], categoryUrls[0]);
        }
      }
    }
    urlCategoryReader.close();
    word_count_threshold = Integer.parseInt(conf.get(UpdateDictDriver.COUNT_THRESHOLD));
    System.out.println("word count lower boundary is " + word_count_threshold);
    word_count_upper_threshold = Integer.parseInt(conf.get(UpdateDictDriver.COUNT_UPPER_THRESHOLD));
    System.out.println("word count upper boundary is " + word_count_upper_threshold);
    String dictRoot = conf.get(UpdateDictDriver.DICT_ROOT);
    System.out.println("dict Root is " + dictRoot);
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
    } else if (word.contains("shop") || word.contains("games")) {
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
