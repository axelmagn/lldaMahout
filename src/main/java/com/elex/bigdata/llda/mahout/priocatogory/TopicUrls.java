package com.elex.bigdata.llda.mahout.priocatogory;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 6/13/14
 * Time: 3:26 PM
 * To change this template use File | Settings | File Templates.
 */
public class TopicUrls {
  private String topic;
  private List<String> urls;

  public TopicUrls(String topic, List<String> urls) {
    this.topic = topic;
    this.urls = urls;
  }
  public TopicUrls(){}

  public String getTopic() {
    return topic;
  }

  public List<String> getUrls() {
    return urls;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public void setUrls(List<String> urls) {
    this.urls = urls;
  }
}
