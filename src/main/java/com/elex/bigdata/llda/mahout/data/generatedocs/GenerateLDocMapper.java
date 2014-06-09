package com.elex.bigdata.llda.mahout.data.generatedocs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 10:36 AM
 * To change this template use File | Settings | File Templates.
 */
public class GenerateLDocMapper extends Mapper<Object,Text,Text,Text> {
  private Set<String> eliminated_urls=new HashSet<String>();
  public void setup(Context context){
    InputStream inputStream = this.getClass().getResourceAsStream("/eliminated_urls");
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    String line = null;
    try {
      while ((line = reader.readLine()) != null) {
        String[] urls = line.split(" ");
        for (String url : urls) {
          eliminated_urls.add(url);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
    /*
        extract uid and url-count from value
        while loopCount < count
           context.write(uid,url)
     */
    String[] uidUrlCount=value.toString().split("\t");
    if(eliminated_urls.contains(uidUrlCount[1]))
      return;
    int count=Integer.parseInt(uidUrlCount[2]);
    for(int i=0;i<count;i++){
      context.write(new Text(uidUrlCount[0]),new Text(uidUrlCount[1]));
    }
  }
}
