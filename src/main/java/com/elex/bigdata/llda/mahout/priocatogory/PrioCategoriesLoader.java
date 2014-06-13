package com.elex.bigdata.llda.mahout.priocatogory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.bloom.BloomFilter;

import java.io.DataInput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 6/13/14
 * Time: 4:24 PM
 * To change this template use File | Settings | File Templates.
 */
public class PrioCategoriesLoader {
  private static String GLOBAL_FILTER = "globalFilter";
  private String inputDir;
  private Map<String, BloomFilter> bloomFilterMap;
  private BloomFilter globalFilter=null;
  private FileSystem fs;
  public static PrioCategoriesLoader prioCategoriesLoader=null;
  private boolean loaded=false;

  private PrioCategoriesLoader(String inputDir, FileSystem fs) {
    this.inputDir = inputDir;
    bloomFilterMap = new HashMap<String, BloomFilter>();
    this.fs = fs;
  }

  public static PrioCategoriesLoader getCategoriesLoader(String inputDir,FileSystem fs){
     if(prioCategoriesLoader==null )
       prioCategoriesLoader=new PrioCategoriesLoader(inputDir,fs);
     return prioCategoriesLoader;
  }

  private void loadFilter(FileSystem fs) throws IOException {
    Path inputPath = new Path(inputDir);
    if (!fs.exists(inputPath) || !fs.isDirectory(inputPath))
      return;
    FileStatus[] fileStatuses = fs.listStatus(inputPath);
    for (FileStatus fileStatus : fileStatuses) {
      String categoryName = fileStatus.getPath().getName();
      DataInput dataInput = fs.open(fileStatus.getPath());
      BloomFilter bloomFilter = new BloomFilter();
      bloomFilter.readFields(dataInput);
      System.out.println("load bloomFilter "+categoryName);
      if (!categoryName.equals(GLOBAL_FILTER))
        bloomFilterMap.put(categoryName, bloomFilter);
      else
      {
        globalFilter = bloomFilter;
        System.out.println("globalFilter set");
      }
    }
    loaded=true;

  }

  public Map<String, BloomFilter> getCategoryFilters() throws IOException {
    if (!loaded)
      loadFilter(fs);
    return bloomFilterMap;
  }

  public BloomFilter getGlobalFilter() throws IOException {
    if (!loaded)
      loadFilter(fs);
    return globalFilter;
  }
}
