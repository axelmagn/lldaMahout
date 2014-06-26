package com.elex.bigdata.llda.mahout.data;

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
    String json="{\"topic\":\"Top/Computers/Usenet\",\"urls\":[\"http://www.yabnews.com/\",\"http://www.faqs.org/usenet/\"]}";
    Map<String,Object> result=mapper.readValue(json,Map.class);
    TopicUrls topicUrls=mapper.readValue(json,TopicUrls.class);
    System.out.println("hhhe");
  }
  public void testStringBuilder(){
    StringBuilder builder=new StringBuilder().append("hello,");
    builder.deleteCharAt(builder.length()-1);

  }
  // url_category
  @Test
  public void changeOdp() throws IOException {
    Map<String,List<String>> categoryUrls=new HashMap<String,List<String>>();
    String categoryRoot="/data/log/user_category/llda/categories/odp";
    File categoryDir=new File(categoryRoot);
    ObjectMapper mapper=new ObjectMapper();
    for(File file: categoryDir.listFiles()){
      BufferedReader reader=new BufferedReader(new FileReader(file));
      String line;
      String category=file.getName();
      List<String> urls=new ArrayList<String>();
      while((line=reader.readLine())!=null){
         TopicUrls topicUrls=mapper.readValue(line,TopicUrls.class);
         urls.addAll(topicUrls.getUrls());
      }
      categoryUrls.put(category,urls);
      reader.close();
    }
    File resultFile=new File(categoryRoot, "dmProduction/result");
    BufferedWriter writer=new BufferedWriter(new FileWriter(resultFile));
    for(Map.Entry<String,List<String>> entry: categoryUrls.entrySet()){
      StringBuilder outLine=new StringBuilder();
      outLine.append(entry.getKey()+" ");
      for(String url: entry.getValue()){
        outLine.append(url+" ");
      }
      outLine.deleteCharAt(outLine.lastIndexOf(" "));
      writer.write(outLine.toString());
      writer.newLine();
    }
    writer.flush();
    writer.close();
  }

  @Test
  public void changeOrignal() throws IOException {
    Map<String,List<String>> categoryUrls=new HashMap<String,List<String>>();
    String categoryRoot="/data/log/user_category/llda/categories/orignal";
    File categoryDir=new File(categoryRoot);
    for(File file: categoryDir.listFiles()){
      BufferedReader reader=new BufferedReader(new FileReader(file));
      String line;
      String category=file.getName();
      List<String> urls=new ArrayList<String>();
      while((line=reader.readLine())!=null){
        urls.add(line.trim());
      }
      categoryUrls.put(category,urls);
      reader.close();
    }
    File resultFile=new File(categoryRoot, "dmProduction/result");
    BufferedWriter writer=new BufferedWriter(new FileWriter(resultFile));
    for(Map.Entry<String,List<String>> entry: categoryUrls.entrySet()){
      StringBuilder outLine=new StringBuilder();
      outLine.append(entry.getKey()+" ");
      for(String url: entry.getValue()){
        outLine.append(url+" ");
      }
      outLine.deleteCharAt(outLine.lastIndexOf(" "));
      writer.write(outLine.toString());
      writer.newLine();
    }
    writer.flush();
    writer.close();
  }

  @Test
  public void mergeCategories() throws IOException {
    Map<String,Set<String>> categoryUrls=new HashMap<String,Set<String>>();
    String categoryRoot="/data/log/user_category/llda/categories/result";
    File categoryDir=new File(categoryRoot);
    for(File file: categoryDir.listFiles()){
      BufferedReader reader=new BufferedReader(new FileReader(file));
      String line;

      while((line=reader.readLine())!=null){
        String[] fields=line.split(" ");
        String category=fields[0];
        Set<String> urls=new HashSet<String>();
        for(int i=1;i<fields.length;i++)
        {
          fields[i].trim();
          if(fields[i].startsWith("http://"))
            fields[i]=fields[i].substring(7);
          if(fields[i].endsWith("/"))
            fields[i]=fields[i].substring(0, fields[i].length() - 1);
          urls.add(fields[i]);
        }
        Set<String> preUrls=categoryUrls.get(category);
        if(preUrls==null){
          preUrls=new HashSet<String>();
          categoryUrls.put(category,preUrls);
        }
        preUrls.addAll(urls);
      }
      reader.close();
    }
    File resultFile=new File(categoryRoot, "dmProduction/result");
    BufferedWriter writer=new BufferedWriter(new FileWriter(resultFile));
    for(Map.Entry<String,Set<String>> entry: categoryUrls.entrySet()){
      StringBuilder outLine=new StringBuilder();
      outLine.append(entry.getKey()+" ");
      for(String url: entry.getValue()){
        outLine.append(url+" ");
      }
      outLine.deleteCharAt(outLine.lastIndexOf(" "));
      writer.write(outLine.toString());
      writer.newLine();
    }
    writer.flush();
    writer.close();
  }
}
