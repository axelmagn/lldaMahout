package com.elex.bigdata.llda.mahout.data.generatedocs;

import com.elex.bigdata.hashing.BDMD5;
import com.elex.bigdata.llda.mahout.dictionary.Dictionary;
import com.elex.bigdata.llda.mahout.mapreduce.etl.ResultEtlDriver;
import com.elex.bigdata.llda.mahout.priocatogory.ParentToChildLabels;
import com.elex.bigdata.llda.mahout.priocatogory.TopicUrls;
import com.elex.bigdata.llda.mahout.util.PrefixTrie;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.mahout.common.Pair;
import org.apache.mahout.math.MultiLabelVectorWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.codehaus.jackson.map.ObjectMapper;

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
  public static final String URL_TOPIC = "url_topic";

  // save urlTopics
  private Map<String, String> url_category_map = new HashMap<String, String>();
  private Map<String, Integer> category_label_map = new HashMap<String, Integer>();
  PrefixTrie prefixTrie = new PrefixTrie();

  // for uid saving
  private SequenceFile.Writer uidWriter;
  private boolean saveUids = false;

  // for statistics
  private int uidNum = 0;
  private int sampleRatio = 10000, index = 0;
  private long timeCost = 0l,labelVectorTimeCost=0l,queryDictTime=0l,md5Time=0l,calTime=0l,ioTime=0l,reduceTime=0l;

  //int termSize;
  public void setup(Context context) throws IOException {
    /*
       load url--category map
       get uid Path(write uid to hdfs) and create a sequenceFileWriter
    */
    Configuration conf = context.getConfiguration();
    initUidSaver(conf);
    Pair<Map<String,String>,Map<String,Integer>> pair = loadUrlTopics(conf,prefixTrie);
    url_category_map=pair.getFirst();
    category_label_map=pair.getSecond();
    log.info("prefixTrie jvm size "+ prefixTrie.getSize()*37*8);
  }

  private void initUidSaver(Configuration conf) throws IOException {
    String uidFile = conf.get(GenerateLDocDriver.UID_PATH);
    if (uidFile != null) {
      saveUids = true;
      Path uidPath = new Path(uidFile);
      FileSystem fs = FileSystem.get(conf);
      if (fs.exists(uidPath))
        fs.delete(uidPath);
      uidWriter = SequenceFile.createWriter(fs, conf, uidPath, Text.class, NullWritable.class);
    }
  }

  private Pair<Map<String,String>,Map<String,Integer>> loadUrlTopics(Configuration conf,PrefixTrie prefixTrie) throws IOException {
    int prefixTrieWordCount=0;
    Map<String,String> url2Category=new HashMap<String, String>();
    Map<String,Integer> category2Label=new HashMap<String, Integer>();
    Set<Integer> destParentLabels=getDestParentLabels();
    Map<Integer,Integer> child2ParentLabels=getLabelRelations(conf);

    ObjectMapper objectMapper=new ObjectMapper();
    FileSystem fs = FileSystem.get(conf);
    Path resourcesPath = new Path(conf.get(GenerateLDocDriver.RESOURCE_ROOT));
    Path urlTopicPath = new Path(resourcesPath, URL_TOPIC);
    BufferedReader urlCategoryReader = new BufferedReader(new InputStreamReader(fs.open(urlTopicPath)));
    String line ;


    while ((line = urlCategoryReader.readLine()) != null) {
      TopicUrls topicUrls=objectMapper.readValue(line,TopicUrls.class);
      if(destParentLabels.contains(child2ParentLabels.get(topicUrls.getLabel()))) {
        for(String url: topicUrls.getUrls()){
          prefixTrie.insert(url,topicUrls.getLabel());
        }
        prefixTrieWordCount+=topicUrls.getUrls().size();
      }else {
        for(String url: topicUrls.getUrls()){
           url2Category.put(url,topicUrls.getTopic());
        }
      }
      category2Label.put(topicUrls.getTopic(),topicUrls.getLabel());
    }
    urlCategoryReader.close();
    log.info("prefix trie word count "+prefixTrieWordCount);
    return new Pair<Map<String, String>, Map<String, Integer>>(url2Category,category2Label);
  }

  private Set<Integer> getDestParentLabels() throws IOException {
     BufferedReader reader=new BufferedReader(new InputStreamReader(
       this.getClass().getResourceAsStream("/"+GenerateLDocDriver.DEST_PARENT_LABELS)));
     Set<Integer> labels=new HashSet<Integer>();
     String line;
     while((line=reader.readLine())!=null){
       labels.add(Integer.parseInt(line.trim()));
     }
    return labels;
  }

  private Map<Integer,Integer> getLabelRelations(Configuration conf) throws IOException {
    Map<Integer,Integer> child2ParentLabelMap=new HashMap<Integer, Integer>();
    FileSystem fs = FileSystem.get(conf);
    Path resourcesPath = new Path(conf.get(GenerateLDocDriver.RESOURCE_ROOT));
    Path labelRelationPath=new Path(resourcesPath, ResultEtlDriver.LABEL_RELATION);
    BufferedReader urlCategoryReader = new BufferedReader(new InputStreamReader(fs.open(labelRelationPath)));
    String line ;
    ObjectMapper objectMapper=new ObjectMapper();
    while ((line = urlCategoryReader.readLine()) != null) {
      ParentToChildLabels parentToChildLabels=objectMapper.readValue(line.trim(),ParentToChildLabels.class);
      for(Integer label: parentToChildLabels.getChildLabels()){
         child2ParentLabelMap.put(label,parentToChildLabels.getParentLabel());
      }
    }
    return child2ParentLabelMap;
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
      String[] tokens = value.toString().split("\t");
      String url = tokens[0];
      boolean hit=true;
      hitCount++;
      int id=Integer.parseInt(tokens[1]);
      int count = Integer.parseInt(tokens[2]);

      String category = url_category_map.get(url);
      if (category == null) {
        int categoryId = prefixTrie.prefixSearch(url);
        if (categoryId != -1)
          labelSet.add(categoryId);
        /*
         special rules according to human assert
       */
        else if(url.contains("games"))
          labelSet.add(1);
        else if((url.contains("shop")|| url.contains("amazon.")))
          labelSet.add(2);
        else
          hit=false;
      }else {
        labelSet.add(category_label_map.get(category));
      }
      // update word count
      if (hit) {
        count=count+5;
      }
      if (urlCounts.containsKey(id))
        urlCounts.put(id, urlCounts.get(id) + count);
      else
        urlCounts.put(id, (double) count);
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
      log.info("md5 use "+md5Time/(1000*1000l));
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
