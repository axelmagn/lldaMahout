package com.elex.bigdata.llda.mahout.data;

import com.elex.bigdata.llda.mahout.priocatogory.ParentToChildLabels;
import com.elex.bigdata.llda.mahout.priocatogory.TopicUrls;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.io.*;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 6/13/14
 * Time: 2:53 PM
 * To change this template use File | Settings | File Templates.
 */
public class JsonTest {
  @Test
  public void testJson() throws IOException {
    ObjectMapper mapper=new ObjectMapper();
    String json="{\"topic\":\"Coin-Op\",\"label\":\"105\",\"urls\":[\"http://1930s.com/pinball/\",\"http://8linesupply.com/\"]}";
    TopicUrls topicUrls=mapper.readValue(json,TopicUrls.class);
    System.out.println(mapper.writeValueAsString(topicUrls));
    json="{\"parent\":1,\"childs\":[1,101,102,103]}";
    ParentToChildLabels parentToChildLabels=mapper.readValue(json,ParentToChildLabels.class);
    System.out.println(mapper.writeValueAsString(parentToChildLabels));
  }

}
