package com.elex.bigdata.llda.mahout.data.generatedocs;

import com.elex.bigdata.llda.mahout.data.LabeledDocument;
import com.elex.bigdata.llda.mahout.data.LabeledDocumentWritable;
import com.elex.bigdata.llda.mahout.dictionary.UpdateDictDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 10:36 AM
 * To change this template use File | Settings | File Templates.
 */
public class GenerateLDocReducer extends Reducer<Text,Text,Text,LabeledDocumentWritable> {
  Map<String,Integer> dictionary=new HashMap<String,Integer>();
  Map<String,String> url_category_map=new HashMap<String,String>();
  Map<String,Integer> category_label_map=new HashMap<String, Integer>();
  SequenceFile.Writer uidWriter;
  int termSize;
  public void setup(Context context) throws IOException {
    /*
       load dict;
       load url--category map
       get uid Path(write uid to hdfs) and create a sequenceFileWriter
    */
    Configuration conf=context.getConfiguration();
    FileSystem fs=FileSystem.get(conf);
    Path dictPath=new Path(conf.get(UpdateDictDriver.DICT_PATH));
    Path urlCategoryPath=new Path(conf.get(GenerateLDocDriver.URL_CATEGORY_PATH));
    Path categoryLabelPath=new Path(conf.get(GenerateLDocDriver.CATEGORY_LABEL_PATH));
    Path uidPath=new Path(conf.get(GenerateLDocDriver.UID_PATH));
    SequenceFile.Reader dictReader=new SequenceFile.Reader(fs,dictPath,conf);
    Text urlText=new Text();
    IntWritable intWritable=new IntWritable();
    while(dictReader.next(urlText,intWritable)){
      dictionary.put(urlText.toString(),intWritable.get());
    }
    dictReader.close();
    termSize=dictionary.size();

    BufferedReader urlCategoryReader=new BufferedReader(new InputStreamReader(fs.open(urlCategoryPath)));
    String line="";
    while((line=urlCategoryReader.readLine())!=null){
      String[] categoryUrls=line.split(" ");
      for(int i=1;i<categoryUrls.length;i++){
        url_category_map.put(categoryUrls[i],categoryUrls[0]);
      }
    }
    urlCategoryReader.close();

    BufferedReader categoryLabelReader=new BufferedReader(new InputStreamReader(fs.open(categoryLabelPath)));
    while((line=categoryLabelReader.readLine())!=null){
      String[] categoryLabels=line.split("=");
      category_label_map.put(categoryLabels[0],Integer.parseInt(categoryLabels[1]));
    }

    uidWriter=SequenceFile.createWriter(fs,conf,uidPath,Text.class, NullWritable.class);
  }
  public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
     /*
       create a SparseVector with size of dictSize
       in loop for values:
          get id of url
          vector set value in index id
          get category url and it's label,then set labels

       uidWriter.write(key,NullWritable)

     */
    Vector urlCounts=new RandomAccessSparseVector(termSize);
    Vector labels=new RandomAccessSparseVector(category_label_map.size());

    for(Text url: values){
      int id=dictionary.get(url.toString());
      urlCounts.set(id,urlCounts.get(id)+1l);
      String category=url_category_map.get(url.toString());
      if(category!=null){
        Integer label=category_label_map.get(category);
        if(label!=null){
           labels.set(label,1);
        }
      }
    }
    LabeledDocument lDoc=new LabeledDocument(labels,urlCounts);
    uidWriter.append(key,NullWritable.get());
    context.write(key,new LabeledDocumentWritable(lDoc));
  }

  public void cleanup(Context context) throws IOException {
    uidWriter.hflush();
    uidWriter.close();
  }
}
