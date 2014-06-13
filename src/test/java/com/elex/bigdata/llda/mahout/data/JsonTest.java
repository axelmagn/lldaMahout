package com.elex.bigdata.llda.mahout.data;

import com.elex.bigdata.llda.mahout.priocatogory.TopicUrls;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

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
    String json="{\"topic\":\"Top/Computers/Usenet\",\"urls\":[\"http://www.yabnews.com/\",\"http://www.faqs.org/usenet/\"]}";
    Map<String,Object> result=mapper.readValue(json,Map.class);
    TopicUrls topicUrls=mapper.readValue(json,TopicUrls.class);
    System.out.println("hhhe");
  }
}
