package com.elex.bigdata.llda.mahout.priocatogory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 6/13/14
 * Time: 2:50 PM
 * To change this template use File | Settings | File Templates.
 */
public class TransPrioCategories {
  private ObjectMapper mapper=new ObjectMapper();
  private BloomFilter globalFilter;
  private String inputFile,outputDir;
  FileSystem fs;
  //for test in localFileSystem
  public static void main(String[] args) throws IOException, URISyntaxException {
    String inputFile=args[0];
    String outputPath=args[1];
    FileSystem fs=new RawLocalFileSystem();
    fs.initialize(new URI("localFs"),new Configuration());
    TransPrioCategories transPrioCategories=new TransPrioCategories(inputFile,outputPath,fs);
    transPrioCategories.transCategories();
  }

  public TransPrioCategories(String inputFile,String outputDir,FileSystem fs){
    this.inputFile=inputFile;
    this.outputDir=outputDir;
    this.fs=fs;
  }

  public void transCategories() throws IOException {
    BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(new Path(inputFile))));
    File dir=new File(outputDir);
    if(!dir.exists())
      dir.mkdirs();
    String line=null;
    int totalUrlCount=0;
    while((line=reader.readLine())!=null){
      TopicUrls topicUrls=mapper.readValue(line,TopicUrls.class);
      int topicUrlCount=topicUrls.getUrls().size();
      BloomFilter filter=new BloomFilter(topicUrlCount*10,2,0);
      totalUrlCount+=topicUrlCount;
      for(String url:topicUrls.getUrls()){
        if(url.startsWith("https://"))
          url=url.substring(7);
        if(url.endsWith("/"))
          url=url.substring(0,url.length()-1);
        filter.add(new Key(Bytes.toBytes(url)));
      }
      String outputFile=outputDir+File.separator+topicUrls.getTopic().replaceAll("/",".");
      DataOutputStream dataOutputStream=fs.create(new Path(outputFile));
      filter.write(dataOutputStream);
    }
    System.out.println(totalUrlCount);
    reader.close();
    reader=new BufferedReader(new FileReader(inputFile));
    globalFilter=new BloomFilter(totalUrlCount*10,2,0);
    while((line=reader.readLine())!=null){
       TopicUrls topicUrls=mapper.readValue(line,TopicUrls.class);
       for(String url: topicUrls.getUrls()){
         if(url.startsWith("https://"))
           url=url.substring(7);
         if(url.endsWith("/"))
           url=url.substring(0,url.length()-1);
         globalFilter.add(new Key(Bytes.toBytes(url)));
       }

    }
    String globalFilterFile=outputDir+File.separator+"globalFilter";
    DataOutputStream globalOutputStream=new DataOutputStream(new FileOutputStream(globalFilterFile));
    globalFilter.write(globalOutputStream);
  }
}
