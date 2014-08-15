package com.elex.bigdata.llda.mahout.priocatogory;

import org.codehaus.jackson.annotate.JsonProperty;

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
  private int label;
  private List<String> urls;

  public TopicUrls(@JsonProperty("topic") String topic, @JsonProperty("label") int label,@JsonProperty("urls") List<String> urls) {
    this.topic = topic;
    this.label=label;
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

  public int getLabel() {
    return label;
  }

  public void setLabel(int label) {
    this.label = label;
  }
}
