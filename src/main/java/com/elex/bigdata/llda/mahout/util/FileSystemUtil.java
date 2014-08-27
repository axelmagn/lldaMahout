package com.elex.bigdata.llda.mahout.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/7/14
 * Time: 4:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileSystemUtil {
  public static long getLen(Configuration conf, Path path) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    return getLen(fs, path);
  }

  public static long getLen(FileSystem fs, Path path) throws IOException {
    FileStatus[] totalStatus = fs.globStatus(path);
    if (totalStatus.length > 1 || !totalStatus[0].getPath().getName().equals(path.getName())) {
      long len = 0l;
      for (FileStatus status : totalStatus) {
        System.out.println(status.getPath().toString());
        len += getLen(fs, status.getPath());
      }
      return len;
    } else {
      if (fs.isFile(path))
        return fs.getFileStatus(path).getLen();
      else {
        FileStatus[] fileStatuses = fs.listStatus(path);
        long len = 0l;
        for (FileStatus fileStatus : fileStatuses) {
          len += getLen(fs, fileStatus.getPath());
        }
        return len;
      }
    }
  }

  public static void setCombineInputSplitSize(Configuration conf, Path inputPath) throws IOException {
    JobClient jobClient = new JobClient(conf);
    ClusterStatus clusterStatus = jobClient.getClusterStatus();
    int maxMapTaskNum = clusterStatus.getMaxMapTasks();
    System.out.println("max Map Task Num " + maxMapTaskNum);
    long totalSize = FileSystemUtil.getLen(conf, inputPath);
    System.out.println("total input Size " + totalSize);
    long maxSplitSize = totalSize / maxMapTaskNum;
    System.out.println("mapred.max.split.size " + maxSplitSize);
    conf.setLong("mapred.max.split.size", maxSplitSize); // 1G
    conf.setLong("mapreduce.input.fileinputformat.split.maxsize", maxSplitSize);
    long minSplitSizePerNode = maxSplitSize / 2;
    System.out.println("mapred.min.split.size.per.node " + minSplitSizePerNode);
    conf.setLong("mapred.min.split.size.per.node", minSplitSizePerNode);
    conf.setLong("mapreduce.input.fileinputformat.split.minsize.per.node", minSplitSizePerNode);
    long minSplitSizePerRack = (maxSplitSize / 3) * 2;
    System.out.println("mapred.min.split.size.per.rack " + minSplitSizePerRack);
    conf.setLong("mapred.min.split.size.per.rack", minSplitSizePerRack);
    conf.setLong("mapreduce.input.fileinputformat.split.minsize.per.rack", minSplitSizePerRack);
  }

  public static void setCombineInputSplitSize(Configuration conf, Path[] inputPaths) throws IOException {
    JobClient jobClient = new JobClient(conf);
    ClusterStatus clusterStatus = jobClient.getClusterStatus();
    int maxMapTaskNum = clusterStatus.getMaxMapTasks();
    System.out.println("max Map Task Num " + maxMapTaskNum);
    long totalSize = 0;
    for (Path inputPath : inputPaths)
      totalSize += FileSystemUtil.getLen(conf, inputPath);
    System.out.println("total input Size " + totalSize);
    long maxSplitSize = totalSize / maxMapTaskNum;
    System.out.println("mapred.max.split.size " + maxSplitSize);
    conf.setLong("mapred.max.split.size", maxSplitSize); // 1G
    conf.setLong("mapreduce.input.fileinputformat.split.maxsize", maxSplitSize);
    long minSplitSizePerNode = maxSplitSize / 2;
    System.out.println("mapred.min.split.size.per.node " + minSplitSizePerNode);
    conf.setLong("mapred.min.split.size.per.node", minSplitSizePerNode);
    conf.setLong("mapreduce.input.fileinputformat.split.minsize.per.node", minSplitSizePerNode);
    long minSplitSizePerRack = (maxSplitSize / 3) * 2;
    System.out.println("mapred.min.split.size.per.rack " + minSplitSizePerRack);
    conf.setLong("mapred.min.split.size.per.rack", minSplitSizePerRack);
    conf.setLong("mapreduce.input.fileinputformat.split.minsize.per.rack", minSplitSizePerRack);
  }

  public static void deleteOutputPath(Configuration conf, Path outputPath) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outputPath))
      fs.delete(outputPath, true);
  }

}
