package com.elex.bigdata.llda.mahout.data.accumulate;

import com.elex.bigdata.llda.mahout.data.hbase.*;
import com.elex.bigdata.llda.mahout.util.FileSystemUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/11/14
 * Time: 5:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class Accumulate extends AbstractJob {
  String outputBase,output;
  private long startTimeStamp, endTimeStamp;
  private String content;
  private Configuration conf;
  // Option string
  private static String OUTPUT_BASE = "outputBase";
  private static String STARTTIME = "startTime";
  private static String ENDTIME = "endTime";
  private static String CONTENT = "content";
  public static String TABLE_TYPE= "table_type";

  private Map<String,SuperTable> table2Type=new HashMap<String, SuperTable>();
  public Accumulate() {

  }

  public Accumulate(String outputBase, String startTime, String endTime,String content) throws ParseException, IOException, IllegalAccessException, InstantiationException, ClassNotFoundException, ParserConfigurationException, SAXException {
    init(outputBase, startTime, endTime,content);
  }

  private void init(String outputBase, String startTime, String endTime,String content) throws ParseException, IOException, ClassNotFoundException, IllegalAccessException, InstantiationException, ParserConfigurationException, SAXException {
    this.outputBase = outputBase;
    output=outputBase+File.separator+startTime+"_"+endTime;
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    startTimeStamp = dateFormat.parse(startTime).getTime();
    endTimeStamp = dateFormat.parse(endTime).getTime();
    conf = HBaseConfiguration.create();
    this.content=content;
    System.out.println("init ");
    loadTableTypeConfig("/table_type.xml", content);
  }

  private void loadTableTypeConfig(String configFile,String content) throws ParserConfigurationException, IOException, SAXException, ClassNotFoundException, IllegalAccessException, InstantiationException {
    URL url=this.getClass().getResource(configFile);
    System.out.println(url.getPath());
    DocumentBuilderFactory documentBuilderFactory=DocumentBuilderFactory.newInstance();
    DocumentBuilder documentBuilder=documentBuilderFactory.newDocumentBuilder();
    Document doc=documentBuilder.parse(url.getPath());
    Element rootElement=doc.getDocumentElement();
    NodeList nodeList=rootElement.getElementsByTagName(content);
    for(int i=0;i<nodeList.getLength();i++){
      Node node=nodeList.item(i);
      if(node.getNodeType()!=Node.ELEMENT_NODE)
        continue;
      NodeList nodes=node.getChildNodes();
      for(int j=0;j<nodes.getLength();j++){
        Node childNode = nodes.item(j);
        if(childNode.getNodeType()!=Node.ELEMENT_NODE)
          continue;
        Element element=(Element)childNode;
        String tableName=element.getTagName();
        String tableTypeName=element.getTextContent();
        SuperTable superTable=(SuperTable)Class.forName(tableTypeName).newInstance();
        table2Type.put(tableName,superTable);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(HBaseConfiguration.create(), new Accumulate(), args);

  }

  @Override
  public int run(String[] args) throws Exception {
    addOption(OUTPUT_BASE, "ob", "output base dir in hdfs");
    addOption(STARTTIME, "st", "start time of url access");
    addOption(ENDTIME, "et", "end time of url access");
    addOption(CONTENT,"content","specify the content ");
    if (parseArguments(args) == null)
      return -1;
    init(getOption(OUTPUT_BASE), getOption(STARTTIME), getOption(ENDTIME),getOption(CONTENT,"url"));
    System.out.println("init complete "+table2Type.size());
    for(Map.Entry<String,SuperTable> entry: table2Type.entrySet()){
      System.out.println(entry.getKey()+":"+entry.getValue().getClass().getName());
      Job job=prepareJob(entry.getKey(),entry.getValue(),startTimeStamp,endTimeStamp);
      job.submit();
      job.waitForCompletion(true);
    }
    return 0;
  }

  public Job prepareJob(String tableName,SuperTable tabletype,long startTime,long endTime) throws IOException, ClassNotFoundException, InterruptedException {
    Path outputPath=new Path(output,tableName);
    FileSystemUtil.deleteOutputPath(conf,outputPath);
    conf.set(TABLE_TYPE,tabletype.getClass().getName());
    Job job=new Job(conf,"accumulate "+tableName+" "+content+" "+outputPath.toString());
    Scan scan=tabletype.getScan(startTime,endTime);
    TableMapReduceUtil.initTableMapperJob(tableName, scan, AccumulateMapper.class, Text.class, Text.class, job);
    if(content.equals("url"))
      job.setNumReduceTasks(0);
    else if(content.equals("nt")){
      job.setReducerClass(AccumulateNtReducer.class);
      job.setNumReduceTasks(4);
    }
    else
      throw new ClassCastException("content is wrong "+content);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setJarByClass(Accumulate.class);
    return job;
  }
}