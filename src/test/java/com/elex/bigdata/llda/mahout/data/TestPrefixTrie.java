package com.elex.bigdata.llda.mahout.data;

import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver;
import com.elex.bigdata.llda.mahout.mapreduce.etl.ResultEtlDriver;
import com.elex.bigdata.llda.mahout.priocatogory.ParentToChildLabels;
import com.elex.bigdata.llda.mahout.priocatogory.TopicUrls;
import com.elex.bigdata.llda.mahout.util.PrefixTrie;
import com.elex.bigdata.llda.mahout.util.Trie;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.Pair;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/11/14
 * Time: 3:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestPrefixTrie {
  @Test
  public void prefixSearch() throws IOException {
    String[] destUrls = new String[]{"www.337.com/en/minigame/play/balls_n_walls","www.jogos.com", "www.jogos.com/myword", "www.neoseeker.com", "www.jogos.com/myword", "backyardscoreboards.com/hello?myapp=12&myid=15"};
    PrefixTrie prefixTrie=new PrefixTrie();
    Pair<Map<String,String>,Map<String,Integer>> pair=loadUrlTopics(prefixTrie);
    for(int i=0;i<destUrls.length;i++)
      System.out.println(prefixTrie.prefixSearch(destUrls[i]));
  }

  public  Pair<Map<String,String>,Map<String,Integer>> loadUrlTopics(PrefixTrie prefixTrie) throws IOException {
    int prefixTrieWordCount=0;
    Map<String,String> url2Category=new HashMap<String, String>();
    Map<String,Integer> category2Label=new HashMap<String, Integer>();
    Set<Integer> destParentLabels=getDestParentLabels();
    Map<Integer,Integer> child2ParentLabels=getLabelRelations();

    ObjectMapper objectMapper=new ObjectMapper();

    BufferedReader urlCategoryReader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/"+"url_topic")));
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
    System.out.println("prefix trie word count "+prefixTrieWordCount);
    return new Pair<Map<String, String>, Map<String, Integer>>(url2Category,category2Label);
  }

  public static Set<Integer> getDestParentLabels() throws IOException {
    BufferedReader reader=new BufferedReader(new InputStreamReader(
      GenerateLDocDriver.class.getResourceAsStream("/"+GenerateLDocDriver.DEST_PARENT_LABELS)));
    Set<Integer> labels=new HashSet<Integer>();
    String line;
    while((line=reader.readLine())!=null){
      labels.add(Integer.parseInt(line.trim()));
    }
    return labels;
  }

  public  Map<Integer,Integer> getLabelRelations() throws IOException {
    Map<Integer,Integer> child2ParentLabelMap=new HashMap<Integer, Integer>();
    BufferedReader urlCategoryReader = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream("/"+"label_relation")));
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
}
