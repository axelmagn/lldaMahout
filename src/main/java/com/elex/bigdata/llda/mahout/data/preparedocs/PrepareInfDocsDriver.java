package com.elex.bigdata.llda.mahout.data.preparedocs;

import com.elex.bigdata.llda.mahout.data.LabeledDocumentWritable;
import com.elex.bigdata.llda.mahout.data.complementdocs.ComplementLDocDriver;
import com.elex.bigdata.llda.mahout.data.complementdocs.ComplementLDocMapper;
import com.elex.bigdata.llda.mahout.data.complementdocs.ComplementLDocReducer;
import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver;
import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocMapper;
import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocReducer;
import com.elex.bigdata.llda.mahout.dictionary.UpdateDictDriver;
import org.apache.hadoop.conf.Configuration;
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

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 5:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class PrepareInfDocsDriver extends AbstractJob{





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
    JobControl jobControl=new JobControl("prepareInfDocs");

    Path textInputPath=getInputPath();
    String dictRoot=getOption(UpdateDictDriver.DICT_OPTION_NAME);
    Configuration conf=getConf();
    setConf(conf);
    Job updateDictJob=UpdateDictDriver.prepareJob(conf,textInputPath,dictRoot);
    updateDictJob.setJobName("updateDict");
    ControlledJob controlledDictJob=new ControlledJob(conf);
    controlledDictJob.setJob(updateDictJob);
    jobControl.addJob(controlledDictJob);

    String docsRoot=getOption(GenerateLDocDriver.DOC_ROOT_OPTION_NAME);
    String docsDir=getOption(GenerateLDocDriver.DOC_OPTION_NAME);
    String docsPath=docsRoot+File.separator+docsDir;
    String uidPath=docsRoot+File.separator+"uid";
    String resourceDir=getOption(GenerateLDocDriver.RESOURCE_OPTION_NAME);

    Job generateDocJob=GenerateLDocDriver.prepareJob(conf,inputPath,new Path(docsPath),dictRoot,resourceDir,uidPath);
    ControlledJob controlledGenLDocJob=new ControlledJob(conf);
    controlledGenLDocJob.setJob(generateDocJob);
    controlledGenLDocJob.addDependingJob(controlledDictJob);
    jobControl.addJob(controlledGenLDocJob);
    // complement docs
    String preLDocPath=docsRoot+File.separator+getOption(ComplementLDocDriver.PRE_LDOC_OPTION_NAME);
    String infLDocPath=docsRoot+File.separator+"inf";
    String currentDocPath=docsPath;
    Job complementDocsJob= ComplementLDocDriver.prepareJob(conf,new Path[]{new Path(preLDocPath),new Path(currentDocPath)},new Path(infLDocPath),uidPath);
    ControlledJob controlledCompDocsJob=new ControlledJob(conf);
    controlledCompDocsJob.setJob(complementDocsJob);
    controlledCompDocsJob.addDependingJob(controlledGenLDocJob);
    jobControl.addJob(controlledCompDocsJob);

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

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(),new PrepareInfDocsDriver(),args);
  }
}
