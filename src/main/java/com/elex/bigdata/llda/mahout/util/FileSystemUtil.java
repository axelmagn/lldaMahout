package com.elex.bigdata.llda.mahout.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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
    if (totalStatus.length > 1) {
      long len=0l;
      for(FileStatus status: totalStatus){
        System.out.println(status.getPath().toString());
        len+=getLen(fs,status.getPath());
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
}
