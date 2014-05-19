package com.elex.bigdata.llda.mahout.data.preparedocs;

import com.elex.bigdata.llda.mahout.data.LabeledDocumentWritable;
import com.elex.bigdata.llda.mahout.data.complementdocs.ComplementLDocDriver;
import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver;
import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocMapper;
import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocReducer;
import com.elex.bigdata.llda.mahout.data.mergedocs.MergeLDocDriver;
import com.elex.bigdata.llda.mahout.data.mergedocs.MergeLDocMapper;
import com.elex.bigdata.llda.mahout.data.mergedocs.MergeLDocReducer;
import com.elex.bigdata.llda.mahout.dictionary.UpdateDictDriver;
import com.elex.bigdata.llda.mahout.dictionary.UpdateDictMapper;
import com.elex.bigdata.llda.mahout.dictionary.UpdateDictReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/15/14
 * Time: 11:33 AM
 * To change this template use File | Settings | File Templates.
 */
public class PrepareEstDocsDriver extends AbstractJob {
  @Override
  public int run(String[] args) throws Exception {
    addInputOption();
    /*
      dictRoot--dict,dictSize,tmpDict
     */
    addOption(UpdateDictDriver.DICT_OPTION_NAME,"dict","dictionary root Path",true);

    /*
      lDocRoot:day--sequenceFile(uidUrlCount seperated by day),total(_day)(total url info to day),inf(the docs to inf)
     */
    addOption(GenerateLDocDriver.DOC_ROOT_OPTION_NAME,"docsRoot","specify the lDocs Root Directory");
    addOption(ComplementLDocDriver.PRE_LDOC_OPTION_NAME,"lIn","InputPath for previous lDocs");
    addOption(GenerateLDocDriver.DOC_OPTION_NAME,"docsDir","specify the lDocs directory");
    /*
      resources:url_category,category_label

     */
    addOption(GenerateLDocDriver.RESOURCE_OPTION_NAME,"rDir","specify the resources Dir");

    if(parseArguments(args)==null){
      return -1;
    }
    List<Job> jobs=new ArrayList<Job>();
    Path textInputPath=getInputPath();
    String dictRoot=getOption(UpdateDictDriver.DICT_OPTION_NAME);
    Configuration conf=getConf();
    setConf(conf);
    Job updateDictJob=UpdateDictDriver.prepareJob(conf,textInputPath,dictRoot);
    jobs.add(updateDictJob);

    String docsRoot=getOption(GenerateLDocDriver.DOC_ROOT_OPTION_NAME);
    String docsDir=getOption(GenerateLDocDriver.DOC_OPTION_NAME);
    String docsPath=docsRoot+File.separator+docsDir;
    String uidPath=docsRoot+File.separator+"uid";
    String resourceDir=getOption(GenerateLDocDriver.RESOURCE_OPTION_NAME);
    Job generateDocJob=GenerateLDocDriver.prepareJob(conf,inputPath,new Path(docsPath),dictRoot,resourceDir,uidPath);
    jobs.add(generateDocJob);

    String estLDocPath=docsRoot+File.separator+"to"+docsDir;
    String preLDocPath=docsRoot+File.separator+getOption(ComplementLDocDriver.PRE_LDOC_OPTION_NAME);
    String currentDocPath=docsPath;
    Job mergeDocsJob= MergeLDocDriver.prepareJob(conf,new Path[]{new Path(preLDocPath),new Path(currentDocPath)},new Path(estLDocPath),dictRoot);
    jobs.add(mergeDocsJob);
    if(handleJobChain(jobs,"prepareEstDocs")==0){
      return 0;
    }
    return -1;
  }

  public static int handleJobChain(List<Job> jobs,String chainName) throws IOException {
    ControlledJob[] controlledJobs=new ControlledJob[jobs.size()];
    for(int i=0;i<jobs.size();i++){
      controlledJobs[i]=new ControlledJob(jobs.get(i).getConfiguration());
      controlledJobs[i].setJob(jobs.get(i));
      if(i!=0)
        controlledJobs[i].addDependingJob(controlledJobs[i-1]);
    }
    JobControl jobControl=new JobControl(chainName);
    for(ControlledJob controlledJob:controlledJobs){
      jobControl.addJob(controlledJob);
    }
    int jobSize=jobControl.getReadyJobsList().size()+jobControl.getWaitingJobList().size();
    System.out.println(jobSize);
    Thread jcThread=new Thread(jobControl);
    jcThread.start();
    while(true){
      if(jobControl.getFailedJobList().size()>0){
        System.out.println("failed job "+jobControl.getFailedJobList());
        jobControl.stop();
        return -1;
      }
      if(jobControl.allFinished()&&jobControl.getSuccessfulJobList().size()>=jobSize){
        System.out.println("successful job "+jobControl.getSuccessfulJobList());
        jobControl.stop();
        return 0;
      }

    }
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new PrepareEstDocsDriver(),args);
  }
}
