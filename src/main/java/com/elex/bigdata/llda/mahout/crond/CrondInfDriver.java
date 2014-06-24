package com.elex.bigdata.llda.mahout.crond;

import com.elex.bigdata.llda.mahout.data.complementdocs.ComplementLDocDriver;
import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver;
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

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/29/14
 * Time: 6:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class CrondInfDriver extends AbstractJob{
  public static final String MODEL_INPUT="model_input";
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
    addOption(GenerateLDocDriver.DOC_ROOT,"dR","docs Root Path");
    //dictionary root path
    addOption(UpdateDictDriver.DICT_ROOT,"dictRoot","dictionary root Path");
    //resources root path
    addOption(GenerateLDocDriver.RESOURCE_DIR,"rDir","specify the resources Dir");
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
    String day=startTime.substring(0,8);
    String Minute=startTime.substring(8,12);

    Path docsRootPath=new Path(getOption(GenerateLDocDriver.DOC_ROOT));
    Path todayDocsPath=new Path(docsRootPath,day);
    Path historyDocsPath=new Path(docsRootPath,"to"+day);
    if(!fs.exists(todayDocsPath))
      fs.mkdirs(todayDocsPath);
    Path currentDocsPath=new Path(todayDocsPath,Minute);
    Path docsForInfPath=new Path(docsRootPath,ComplementLDocDriver.DOC_INF_DIR);

    Path dictRootPath=new Path(getOption(UpdateDictDriver.DICT_ROOT));

    Path resourceRootPath=new Path(getOption(GenerateLDocDriver.RESOURCE_DIR));

    Path uidFilePath=new Path(docsRootPath,GenerateLDocDriver.UID_FILE);

    JobControl jobControl=new JobControl("crondInf");

    Job generateLDocJob=GenerateLDocDriver.prepareJob(conf,inputPath,currentDocsPath,dictRootPath.toString(),resourceRootPath.toString(),uidFilePath.toString());
    ControlledJob controlledGenJob=new ControlledJob(conf);
    controlledGenJob.setJob(generateLDocJob);
    jobControl.addJob(controlledGenJob);

    Job complementJob= ComplementLDocDriver.prepareJob(conf,new Path[]{historyDocsPath,todayDocsPath},docsForInfPath,uidFilePath.toString());
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
