package com.elex.bigdata.llda.mahout.data.mergedocs;

import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.math.MultiLabelVectorWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.map.OpenIntDoubleHashMap;

import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 10:12 AM
 * To change this template use File | Settings | File Templates.
 */
public class MergeLDocDriver extends AbstractJob {
  public static final String MULTI_INPUT="multi_input";
  /*
     InputPaths: labeledDocs files
     conf.setDictSizePath
     MergeLDocMapper:
        create a new labeledDocument with size of dictSize and clone from value
     MergeLDocReducer:
        merge labels and urlCounts for uid:Iterable<MultiLabelVector>
        context.write(uid,labeledDocument)
   */
  @Override
  public int run(String[] args) throws Exception {
    addOption(MULTI_INPUT, "mI", "specify the input Path", true);
    addOption(GenerateLDocDriver.UID_PATH,"uidPath","specify the uid file Path");
    addOutputOption();
    if(parseArguments(args)==null){
      return -1;
    }
    String multiInputs=getOption(MULTI_INPUT);
    String[] inputs=multiInputs.split(":");
    Path[] inputPaths=new Path[inputs.length];
    for(int i=0;i<inputs.length;i++)
       inputPaths[i]=new Path(inputs[i]);
    outputPath=getOutputPath();
    Configuration conf=new Configuration();
    String uid_file_path=getOption(GenerateLDocDriver.UID_PATH);
    if(uid_file_path!=null)
      conf.set(GenerateLDocDriver.UID_PATH,uid_file_path);
    else
      System.out.println("uid_file_path is null");
    Job mergeLDocJob=prepareJob(conf,inputPaths,outputPath);
    mergeLDocJob.submit();
    mergeLDocJob.waitForCompletion(true);
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }

  public static Job prepareJob(Configuration conf,Path[] inputPaths,Path outputPath) throws IOException {
    FileSystem fs=FileSystem.get(conf);
    if(fs.exists(outputPath))
      fs.delete(outputPath);
    Job job=new Job(conf);
    job.setNumReduceTasks(1);
    job.setMapperClass(MergeLDocMapper.class);
    job.setReducerClass(MergeLDocReducer.class);
    for(Path inputPath: inputPaths){
      FileInputFormat.addInputPath(job, inputPath);
    }
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(MultiLabelVectorWritable.class);
    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MultiLabelVectorWritable.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(job,outputPath);
    job.setJobName("merge docs");
    job.setJarByClass(MergeLDocDriver.class);
    return job;
  }

  private Path[] getAdaptivePath(Configuration conf,Path[] orignalPaths) throws IOException {
    FileSystem fs=FileSystem.get(conf);
    List<Path> finalPaths=new ArrayList<Path>();
    for(Path origPath: orignalPaths){
      if(!fs.isDirectory(origPath))
        continue;
      FileStatus[] statuses=fs.listStatus(origPath,new PathFilter() {
        @Override
        public boolean accept(Path path) {
          return !path.getName().equals("_logs");
        }
      });
      boolean containFile=false;
      for(FileStatus fileStatus:statuses){
        if(fileStatus.isFile())
        {
          containFile=true;
          break;
        }
      }
      if(containFile)
        finalPaths.add(origPath);
      else
        for(FileStatus fileStatus:statuses)
          finalPaths.add(fileStatus.getPath());
    }
    return finalPaths.toArray(new Path[finalPaths.size()]);
  }
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new MergeLDocDriver(), args);
  }
  public static MultiLabelVectorWritable mergeDocs(List<MultiLabelVectorWritable> lDocs){
    Set<Integer> labelSet=new HashSet<Integer>();
    OpenIntDoubleHashMap urlCountMap=new OpenIntDoubleHashMap();

    for(int i=0;i<lDocs.size();i++){
      MultiLabelVectorWritable labelVectorWritable=lDocs.get(i);
      for(Integer label: labelVectorWritable.getLabels())
        labelSet.add(label);
      Vector tmpUrlCounts=lDocs.get(i).getVector();
      Iterator<Vector.Element> tmpUrlCountIter=tmpUrlCounts.iterateNonZero();
      while(tmpUrlCountIter.hasNext()){
        Vector.Element urlCount=tmpUrlCountIter.next();
        int termIndex=urlCount.index();
        urlCountMap.put(termIndex,urlCount.get()+urlCountMap.get(termIndex));
      }
    }
    Vector finalUrlCounts=new RandomAccessSparseVector(urlCountMap.size()*2);
    for(Integer url: urlCountMap.keys().elements()){
      finalUrlCounts.setQuick(url,urlCountMap.get(url));
    }
    int[] labels=new int[labelSet.size()];
    int i=0;
    for(Integer label: labelSet)
      labels[i++]=label;
    return new MultiLabelVectorWritable(finalUrlCounts,labels);
  }
}
