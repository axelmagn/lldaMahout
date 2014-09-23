package com.elex.bigdata.llda.mahout.data.generatedocs;

import com.elex.bigdata.hashing.BDMD5;
import com.elex.bigdata.hashing.HashingException;
import com.elex.bigdata.llda.mahout.dictionary.Dictionary;
import com.elex.bigdata.llda.mahout.dictionary.UpdateDictDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

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
  private Logger log=Logger.getLogger(GenerateLDocMapper.class);
  // urls which should be removed
  private Set<String> eliminated_urls=new HashSet<String>();
  private int index=0,sampleRatio=100000;
  // md5 change url to md5Str
  private BDMD5 bdmd5;
  //word dictionary
  private Dictionary dict;
  public void setup(Context context) throws IOException {
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
    Configuration conf=context.getConfiguration();
    FileSystem fs=FileSystem.get(conf);
    try {
      bdmd5=BDMD5.getInstance();
      dict = new Dictionary(conf.get(UpdateDictDriver.DICT_ROOT), fs, conf);
    } catch (HashingException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
  public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
    /*
        extract uid and url-count from value
        while loopCount < count
           context.write(uid,url)
     */
    String[] uidUrlCount=value.toString().split("\t");
    if(uidUrlCount.length<3)
    {
      System.out.println(value.toString()+"   len<3");
      return;
    }
    if(eliminated_urls.contains(uidUrlCount[1]))
      return;
    /*
    if((++index)>sampleRatio)
      log.info(value.toString() + " count is " + count);
      */
    //get key part of url
    String url=uidUrlCount[1];
    int index=url.indexOf('?');
    if(index!=-1)
      url=url.substring(0,index);
    int frequent=0;
    for(int i=0;i<url.length();i++){
      if(url.charAt(i)=='/'){
        frequent++;
        if(frequent==3){
          url=url.substring(0,i);
          break;
        }
      }
    }
    // if dict contains urlMd5,then write
    try {
      String urlMd5=bdmd5.toMD5(url).substring(UpdateDictDriver.MD5_START_INDEX,UpdateDictDriver.MD5_END_INDEX);
      if(!dict.contains(urlMd5))
        return;
      context.write(new Text(uidUrlCount[0].toLowerCase()),new Text(url+"\t"+dict.getId(urlMd5)+"\t"+uidUrlCount[2]));
    } catch (HashingException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }
}
