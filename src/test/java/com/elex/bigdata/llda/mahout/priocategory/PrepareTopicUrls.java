package com.elex.bigdata.llda.mahout.priocategory;

import com.elex.bigdata.llda.mahout.priocatogory.TopicUrls;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/15/14
 * Time: 11:41 AM
 * To change this template use File | Settings | File Templates.
 */
public class PrepareTopicUrls {
  @Test
  public void prepareTopicUrls() throws IOException {
     Set<String> containedUrls=new HashSet<String>();
     List<TopicUrls> topicUrlsList=new ArrayList<TopicUrls>();
     loadJson("/data/log/user_category/llda/categories/result",containedUrls,topicUrlsList);
     loadPlain("/data/log/user_category/llda/categories/result",containedUrls,topicUrlsList);
     String resultFile="/data/log/user_category/llda/categories/result/urlTopic";
     BufferedWriter writer=new BufferedWriter(new FileWriter(resultFile));
     ObjectMapper objectMapper=new ObjectMapper();
     for(TopicUrls topicUrls: topicUrlsList){
        writer.write(objectMapper.writeValueAsString(topicUrls));
        writer.newLine();
     }
     writer.close();
  }

  public Set<String> loadJson(String fileName, Set<String> containedUrls, List<TopicUrls> topicUrlsList) throws IOException {
    Set<String> urls = new HashSet<String>();
    BufferedReader reader = new BufferedReader(new FileReader(fileName));
    ObjectMapper objectMapper = new ObjectMapper();
    String line;
    while ((line = reader.readLine()) != null) {
      //System.out.println(line);
      TopicUrls topicUrls = objectMapper.readValue(line, TopicUrls.class);
      //System.out.println("read complete");
      topicUrlsList.add(topicUrls);
      List<String> processedUrls=new ArrayList<String>();
      for (String url : topicUrls.getUrls()) {
        url.trim();
        if (url.contains("://"))
          url = url.substring(url.indexOf("://") + 3);
        if (url.endsWith("/"))
          url = url.substring(0, url.length() - 1);
        urls.add(url);
        processedUrls.add(url);
        if (!containedUrls.contains(url))
          processedUrls.add(url);
      }
      topicUrls.setUrls(processedUrls);
    }
    reader.close();
    return urls;
  }

  public Set<String> loadPlain(String fileName, Set<String> containedUrls, List<TopicUrls> topicUrlsList) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(fileName));
    Set<String> urls = new HashSet<String>();
    String line;
    while ((line = reader.readLine()) != null) {
      List<String> urlList = new ArrayList<String>();
      String[] tokens = line.split(" ");
      for (int i = 2; i < tokens.length; i++) {
        String url = tokens[i];
        url.trim();
        if (url.contains("://"))
          url = url.substring(url.indexOf("://") + 3);
        if (url.endsWith("/"))
          url = url.substring(0, url.length() - 1);
        if (!containedUrls.contains(url))
        {
          urlList.add(url);
          urls.add(url);
        }
      }
      boolean containedTopic = false;
      for (TopicUrls topicUrls : topicUrlsList) {
        if (topicUrls.getLabel()==Integer.parseInt(tokens[1])) {
          topicUrls.getUrls().addAll(urlList);
          containedTopic = true;
          break;
        }
      }
      if (!containedTopic) {
        TopicUrls topicUrls = new TopicUrls(tokens[0], Integer.parseInt(tokens[1]), urlList);
        topicUrlsList.add(topicUrls);
      }
    }
    reader.close();
    return urls;
  }
}
