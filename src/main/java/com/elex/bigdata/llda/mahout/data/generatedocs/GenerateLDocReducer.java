package com.elex.bigdata.llda.mahout.data.generatedocs;

import com.elex.bigdata.hashing.BDMD5;
import com.elex.bigdata.hashing.HashingException;
import com.elex.bigdata.llda.mahout.dictionary.Dictionary;
import com.elex.bigdata.llda.mahout.dictionary.UpdateDictDriver;
import com.elex.bigdata.llda.mahout.util.PrefixTrie;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.mahout.math.MultiLabelVectorWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 10:36 AM
 * To change this template use File | Settings | File Templates.
 */
public class GenerateLDocReducer extends Reducer<Text, Text, Text, MultiLabelVectorWritable> {
  private Logger log = Logger.getLogger(GenerateLDocReducer.class);
  public static final String URL_CATEGORY = "url_category";
  public static final String CATEGORY_LABEL = "category_label";
  private Dictionary dict;
  private BDMD5 bdmd5;
  private Map<String, String> url_category_map = new HashMap<String, String>();
  private Map<String, Integer> category_label_map = new HashMap<String, Integer>();
  PrefixTrie prefixTrie = new PrefixTrie();
  private String[] destCategories = new String[]{"jogos", "compras", "Friends", "Tourism"};
  Map<String, Integer> categoryIdMap = new HashMap<String, Integer>();
  Map<Integer, String> idCategoryMap = new HashMap<Integer, String>();
  private SequenceFile.Writer uidWriter;
  private boolean saveUids = false;
  private int uidNum = 0;
  private int sampleRatio = 10000, index = 0;
  private long timeCost = 0l,labelVectorTimeCost=0l,queryDictTime=0l,calTime=0l,ioTime=0l,reduceTime=0l;

  //int termSize;
  public void setup(Context context) throws IOException {
    /*
       load dict;
       load url--category map
       get uid Path(write uid to hdfs) and create a sequenceFileWriter
    */
    Configuration conf = context.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    Path resourcesPath = new Path(conf.get(GenerateLDocDriver.RESOURCE_ROOT));
    Path urlCategoryPath = new Path(resourcesPath, URL_CATEGORY);
    Path categoryLabelPath = new Path(resourcesPath, CATEGORY_LABEL);

    for (int i = 0; i < destCategories.length; i++) {
      categoryIdMap.put(destCategories[i], i);
      idCategoryMap.put(i, destCategories[i]);
    }

    String uidFile = conf.get(GenerateLDocDriver.UID_PATH);
    if (uidFile != null) {
      saveUids = true;
      Path uidPath = new Path(uidFile);
      if (fs.exists(uidPath))
        fs.delete(uidPath);
      uidWriter = SequenceFile.createWriter(fs, conf, uidPath, Text.class, NullWritable.class);
    }
    try {
      bdmd5 = BDMD5.getInstance();
      dict = new Dictionary(conf.get(UpdateDictDriver.DICT_ROOT), fs, conf);
    } catch (HashingException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    //termSize=dictionary.size();

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

    BufferedReader categoryLabelReader = new BufferedReader(new InputStreamReader(fs.open(categoryLabelPath)));
    while ((line = categoryLabelReader.readLine()) != null) {
      String[] categoryLabels = line.split("=");
      category_label_map.put(categoryLabels[0], Integer.parseInt(categoryLabels[1]));
    }
  }

  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
     /*
       create a SparseVector with size of dictSize
       in loop for values:
          get id of url
          vector set value in index id
          get category url and it's label,then set labels

       uidWriter.write(key,NullWritable)

     */
    long reduceStart=System.nanoTime();
    int hitCount = 0;
    Map<Integer, Double> urlCounts = new HashMap<Integer, Double>();
    Set<Integer> labelSet = new HashSet<Integer>();
    long startTime;
    for (Text value : values) {
      startTime=System.nanoTime();
      String wordMd5 = null;
      String[] tokens = value.toString().split("\t");
      String url = tokens[0];

      try {
        wordMd5 = bdmd5.toMD5(url.toString());
      } catch (HashingException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
      queryDictTime+=(System.nanoTime()-startTime);
      if (!dict.contains(wordMd5))
        continue;
      startTime=System.nanoTime();
      hitCount++;
      int id = dict.getId(wordMd5);
      int count = Integer.parseInt(tokens[1]);
      if (urlCounts.containsKey(id))
        urlCounts.put(id, urlCounts.get(id) + count);
      else
        urlCounts.put(id, (double) count);
      calTime+=(System.nanoTime()-startTime);
      startTime=System.nanoTime();
      String category = url_category_map.get(url.toString());
      if (category == null) {
        int categoryId = prefixTrie.prefixSearch(url.toString());
        if (categoryId != -1)
          category = idCategoryMap.get(id);
      }
      timeCost+=(System.nanoTime()-startTime);
      startTime=System.nanoTime();
      if (category != null) {
        Integer label = category_label_map.get(category);
        if (label != null) {
          labelSet.add(label);
        }
      }
      labelVectorTimeCost+=(System.nanoTime()-startTime);
    }
    //
    if (urlCounts.size() == 0)
      return;
    startTime=System.nanoTime();
    Vector urlCountsVector = new RandomAccessSparseVector(urlCounts.size() * 2);
    for (Map.Entry<Integer, Double> urlCount : urlCounts.entrySet()) {
      urlCountsVector.setQuick(urlCount.getKey(), urlCount.getValue());
    }
    int[] labels = new int[labelSet.size()];
    int i = 0;
    for (Integer label : labelSet) {
      labels[i++] = label;
    }
    MultiLabelVectorWritable labelVectorWritable = new MultiLabelVectorWritable(urlCountsVector, labels);
    uidNum++;
    labelVectorTimeCost+=(System.nanoTime()-startTime);
    startTime=System.nanoTime();
    if (saveUids)
      uidWriter.append(key, NullWritable.get());
    context.write(key, labelVectorWritable);
    ioTime+=(System.nanoTime()-startTime);
    reduceTime+=(System.nanoTime()-reduceStart);
    if ((++index) >= sampleRatio) {
      log.info("prefixTrie cost "+timeCost/(1000*1000l));
      log.info("produce label vector cost "+labelVectorTimeCost/(1000*1000l));
      log.info("query Dict  use "+queryDictTime/(1000*1000l));
      log.info("cal count use "+calTime/(1000*1000l));
      log.info("io use "+ioTime/(1000*1000l));
      log.info("reduce use "+reduceTime/(1000*1000));
      log.info("hitCount is " + hitCount);
      log.info(" uidNum " + uidNum);
      index = 0;
      /*
      StringBuilder vectorStr = new StringBuilder();
      Iterator<Vector.Element> iterator = urlCountsVector.iterateNonZero();
      while (iterator.hasNext()) {
        Vector.Element e = iterator.next();
        vectorStr.append(e.index() + ":" + e.get() + "  ");
      }
      log.info("vector is : " + vectorStr.toString());
      */
      StringBuilder labelStr = new StringBuilder();
      for (int label : labels) {
        labelStr.append(label + " ");
      }
      log.info("labels is: " + labelStr.toString());
    }
  }

  public void cleanup(Context context) throws IOException {
    log.info("prefixTrie cost "+timeCost/(1000*1000l));
    log.info("produce label vector cost "+labelVectorTimeCost/(1000*1000l));
    log.info("query Dict  use "+queryDictTime/(1000*1000l));
    log.info("cal count use "+calTime/(1000*1000l));
    log.info("io use "+ioTime/(1000*1000l));
    log.info("reduce use "+reduceTime/(1000*1000));
    log.info(" uidNum " + uidNum);
    if (saveUids) {
      uidWriter.hflush();
      uidWriter.close();
    }
  }
}
