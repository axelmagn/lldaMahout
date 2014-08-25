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
  private String rootDir = "/data/log/user_category/llda/categories/result";

  @Test
  public void prepareTopicUrls() throws IOException {
    Set<String> containedUrls = new HashSet<String>();
    List<TopicUrls> topicUrlsList = new ArrayList<TopicUrls>();
    String[] jsonFiles = new String[]{rootDir + "/odp_game_16.txt", rootDir + "/urlTopic"};
    String[] plainFiles = new String[]{rootDir + "/337.csv"};
    for (String jsonFile : jsonFiles)
      loadJson(jsonFile, containedUrls, topicUrlsList);
    for (String plainFile : plainFiles)
      loadPlain(plainFile, containedUrls, topicUrlsList);
    String resultFile = "/data/log/user_category/llda/categories/result/url_topic";
    BufferedWriter writer = new BufferedWriter(new FileWriter(resultFile));
    ObjectMapper objectMapper = new ObjectMapper();
    for (TopicUrls topicUrls : topicUrlsList) {
      writer.write(objectMapper.writeValueAsString(topicUrls));
      writer.newLine();
    }
    writer.close();
  }

  public void loadJson(String fileName, Set<String> containedUrls, List<TopicUrls> topicUrlsList) throws IOException {
    Set<String> urls = new HashSet<String>();
    BufferedReader reader = new BufferedReader(new FileReader(fileName));
    ObjectMapper objectMapper = new ObjectMapper();
    String line;
    while ((line = reader.readLine()) != null) {
      List<String> urlList = new ArrayList<String>();
      //System.out.println(line);
      TopicUrls tmpTopicUrls = objectMapper.readValue(line, TopicUrls.class);
      //System.out.println("read complete");
      for (String url : tmpTopicUrls.getUrls()) {
        url.trim();
        if (url.contains("://"))
          url = url.substring(url.indexOf("://") + 3);
        if (url.endsWith("/"))
          url = url.substring(0, url.length() - 1);
        urlList.add(url);
        if (!containedUrls.contains(url))
        {
          urlList.add(url);
          containedUrls.add(url);
        }
      }
      boolean containedTopic = false;
      for (TopicUrls topicUrls : topicUrlsList) {
        if (topicUrls.getLabel() == tmpTopicUrls.getLabel()) {
          topicUrls.getUrls().addAll(urlList);
          containedTopic = true;
          break;
        }
      }
      if (!containedTopic) {
        tmpTopicUrls.setUrls(urlList);
        topicUrlsList.add(tmpTopicUrls);
      }
    }
    reader.close();
  }

  public void loadPlain(String fileName, Set<String> containedUrls, List<TopicUrls> topicUrlsList) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(fileName));
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
        if (!containedUrls.contains(url)) {
          urlList.add(url);
          containedUrls.add(url);
        }
      }
      boolean containedTopic = false;
      for (TopicUrls topicUrls : topicUrlsList) {
        if (topicUrls.getLabel() == Integer.parseInt(tokens[1])) {
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
  }
}
