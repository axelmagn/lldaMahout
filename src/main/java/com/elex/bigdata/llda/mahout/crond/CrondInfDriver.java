package com.elex.bigdata.llda.mahout.crond;

import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver;
import com.elex.bigdata.llda.mahout.data.mergedocs.MergeLDocDriver;
import com.elex.bigdata.llda.mahout.dictionary.UpdateDictDriver;
import com.elex.bigdata.llda.mahout.mapreduce.LLDADriver;
import com.elex.bigdata.llda.mahout.mapreduce.LLDAInfDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/29/14
 * Time: 6:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class CrondInfDriver extends AbstractJob{
  public static final String MODEL_INPUT="model_input";
  public static final String DOC_ROOT="doc_root";
  public static final String INF_DOC_DIR ="inf";
  private double default_term_topic_smoothing=0.01;
  private double default_doc_topic_smoothing=0.2;

  public CrondInfDriver(){

  }

  @Override
  public int run(String[] args) throws Exception {
    /*
       docs/
            day/hourMinute  : generated lDocs
            toDay           : history lDocs
            inf             : complemented lDocs for inf
            uid             : created when generate lDocs
       docTopics/
            inf             : save result for inf
            est             : save result for est
       models/
       tmpModels/
       resources/
       dictionary/
     */
    /*
       updateDictInput,dictionaryRoot;textInput(orignal format),docsRoot(multiLabelVectorWritable format),docsDir(relative current docs dir)
       generate lDoc to docsDir from text input together with uids;
       complement lDocs in docsDir and output to infDir;
       inf doc in infDir;

     */
    // text input path
    addInputOption();
    //lDocs root path
    addOption(DOC_ROOT,"dR","docs Root Path");
    //dictionary root path
    addOption(UpdateDictDriver.DICT_ROOT,"dictRoot","dictionary root Path");
    //resources root path
    addOption(GenerateLDocDriver.RESOURCE_ROOT,"rDir","specify the resources Dir");
    //num of topics
    addOption(LLDADriver.NUM_TOPICS,"numTopics","specify topic nums ",true);
    //model path
    addOption(MODEL_INPUT,"mI","specify model Path",true);
    //docTopics output Root
    addOutputOption();

    if(parseArguments(args)==null){
      return -1;
    }
    Configuration conf=new Configuration();
    FileSystem fs=FileSystem.get(conf);

    Path inputPath=getInputPath();
    String startTime=inputPath.getName().split("_")[0];
    String endTime=inputPath.getName().split("_")[1];
    String day=startTime.substring(0,8);
    String Minute=startTime.substring(8,12);
    String nextMinute=endTime.substring(8,12);

    SimpleDateFormat dateFormat=new SimpleDateFormat("yyyyMMdd");
    Date date=dateFormat.parse(day);
    date.setDate(date.getDate()-1);
    String oneDayAgo=dateFormat.format(date);
    date.setDate(date.getDate()-1);
    String twoDayAgo=dateFormat.format(date);

    Path docsRootPath=new Path(getOption(DOC_ROOT));

    Path todayDocsPath=new Path(docsRootPath,day+ File.separator+"*");
    Path historyDocsPath=new Path(docsRootPath,"to"+oneDayAgo);
    Path backupHistDocsPath=new Path(docsRootPath,"to"+twoDayAgo);
    if(!fs.exists(todayDocsPath))
      fs.mkdirs(todayDocsPath);

    Path currentDocsPath=new Path(todayDocsPath,Minute+"_"+nextMinute);
    Path docsForInfPath=new Path(docsRootPath, INF_DOC_DIR);

    Path dictRootPath=new Path(getOption(UpdateDictDriver.DICT_ROOT));

    Path resourceRootPath=new Path(getOption(GenerateLDocDriver.RESOURCE_ROOT));

    Path uidFilePath=new Path(docsRootPath,GenerateLDocDriver.UID_FILE);

    JobControl jobControl=new JobControl("crondInf");

    Job generateLDocJob=GenerateLDocDriver.prepareJob(conf,inputPath,dictRootPath,resourceRootPath,currentDocsPath,uidFilePath);
    ControlledJob controlledGenJob=new ControlledJob(conf);
    controlledGenJob.setJob(generateLDocJob);
    jobControl.addJob(controlledGenJob);

    List<Path> comJobInputPaths=new ArrayList<Path>();
    comJobInputPaths.add(todayDocsPath);
    if(!fs.exists(historyDocsPath))
    {
      comJobInputPaths.add(backupHistDocsPath);
      comJobInputPaths.add(new Path(docsRootPath,oneDayAgo));
    }else{
      comJobInputPaths.add(historyDocsPath);
    }

    conf.set(GenerateLDocDriver.UID_PATH,uidFilePath.toString());
    Job complementJob= MergeLDocDriver.prepareJob(conf, comJobInputPaths.toArray(new Path[comJobInputPaths.size()]), docsForInfPath);
    ControlledJob controlledComplementJob=new ControlledJob(conf);
    controlledComplementJob.setJob(complementJob);
    controlledComplementJob.addDependingJob(controlledGenJob);
    jobControl.addJob(controlledComplementJob);

    Path modelInputPath= new Path(getOption(MODEL_INPUT));
    Path docTopicPath= getOutputPath();
    Job infJob=LLDAInfDriver.prepareJob(conf,docsForInfPath,modelInputPath,docTopicPath);
    setInfConf(infJob);
    ControlledJob controlledInfJob=new ControlledJob(conf);
    controlledInfJob.setJob(infJob);
    controlledInfJob.addDependingJob(controlledComplementJob);
    jobControl.addJob(controlledInfJob);

    Thread jcThread=new Thread(jobControl);
    jcThread.start();
    while(true){
      if(jobControl.allFinished()){
        System.out.println(jobControl.getSuccessfulJobList());
        jobControl.stop();
        return 0;
      }
      if(jobControl.getFailedJobList().size()>0){
        System.out.println(jobControl.getFailedJobList());
        jobControl.stop();
        return 1;
      }
    }

  }

  private void setInfConf(Job infJob) throws IOException {
    Configuration conf= infJob.getConfiguration();
    conf.set(LLDADriver.NUM_TOPICS, getOption(LLDADriver.NUM_TOPICS));
    conf.set(LLDADriver.DOC_TOPIC_SMOOTHING,String.valueOf(default_doc_topic_smoothing));
    conf.set(LLDADriver.TERM_TOPIC_SMOOTHING,String.valueOf(default_term_topic_smoothing));
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new CrondInfDriver(),args);
  }
}
