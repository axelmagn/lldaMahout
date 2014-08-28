package com.elex.bigdata.llda.mahout.data.accumulateurlcount.tables;

import com.elex.bigdata.llda.mahout.data.accumulateurlcount.RecordUnit;
import com.elex.bigdata.util.MetricMapping;
import com.xingcloud.xa.hbase.filter.SkipScanFilter;
import com.xingcloud.xa.hbase.model.KeyRange;
import com.xingcloud.xa.hbase.model.KeyRangeComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/28/14
 * Time: 11:36 AM
 * To change this template use File | Settings | File Templates.
 */
public class NavTable implements SuperTable{
  private static byte[] family = Bytes.toBytes("basis");
  private static byte[] URL = Bytes.toBytes("url");
  private static int UID_INDEX = 17;
  public Scan getScan(long startTime, long endTime) {
    Map<String, List<String>> familyColumns = new HashMap<String, List<String>>();
    List<String> columns = new ArrayList<String>();
    columns.add(Bytes.toString(URL));
    familyColumns.put(Bytes.toString(family), columns);
    return getScan(familyColumns,startTime,endTime);
  }

  private Scan getScan(Map<String, List<String>> familyColumns, long startTime,long endTime) {
    List<KeyRange> keyRanges = getSortedKeyRanges(startTime,endTime);
    Scan scan = new Scan();
    Filter filter = new SkipScanFilter(keyRanges);
    scan.setFilter(filter);
    scan.setStartRow(keyRanges.get(0).getLowerRange());
    scan.setStopRow(keyRanges.get(keyRanges.size() - 1).getUpperRange());
    int cacheSize = 10000;
    scan.setCaching(cacheSize);
    for (Map.Entry<String, List<String>> entry : familyColumns.entrySet()) {
      String family = entry.getKey();
      for (String column : entry.getValue()) {
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
      }
    }

    return scan;
  }

  private List<KeyRange> getSortedKeyRanges(long startTime,long endTime) {
    List<KeyRange> keyRanges = new ArrayList<KeyRange>();
    List<String> projects = new ArrayList<String>();
    //todo
    //list all projects and add to list projects
    for (String project : MetricMapping.getInstance().getAllProjectShortNameMapping().keySet())
      projects.add(project);
    Map<Byte, String> projectMap = new HashMap<Byte, String>();
    for (String proj : projects) {
      Byte projectId = MetricMapping.getInstance().getProjectURLByte(proj);
      projectMap.put(projectId, proj);
      Set<String> nations = new HashSet<String>();
      System.out.println("projectId " + projectId + " project: " + proj);
      //todo
      //get nations according to proj and execute the runner.
      long t3 = System.currentTimeMillis();
      Set<String> nationSet = MetricMapping.getNationsByProjectID(projectId);
      for (String nation : nationSet) {
        nations.add(nation);
      }
      System.out.println("get nations use " + (System.currentTimeMillis() - t3) + " ms");

      if (nations.size() != 0 && projectId != null) {
        keyRanges.addAll(getKeyRanges(proj, nations, startTime,endTime));
      }
    }
    KeyRangeComparator comparator = new KeyRangeComparator();
    Collections.sort(keyRanges, comparator);
    /*
    for (KeyRange keyRange : keyRanges) {
      System.out.println("add keyRange " + Bytes.toStringBinary(keyRange.getLowerRange()) + "---" + Bytes.toStringBinary(keyRange.getUpperRange()));
    }
    */
    return keyRanges;
  }

  private List<KeyRange> getKeyRanges(String project, Set<String> nations, long startTime,long endTime) {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    Date startDate=new Date(startTime),endDate=new Date(endTime);
    List<KeyRange> keyRangeList = new ArrayList<KeyRange>();
    for (String nation : nations) {
      byte[] startRk = Bytes.add(new byte[]{MetricMapping.getInstance().getProjectURLByte(project)}, Bytes.toBytes(nation), Bytes.toBytes(dateFormat.format(startDate)));
      byte[] endRk = Bytes.add(new byte[]{MetricMapping.getInstance().getProjectURLByte(project)}, Bytes.toBytes(nation), Bytes.toBytes(dateFormat.format(endDate)));
      KeyRange keyRange = new KeyRange(startRk, true, endRk, false);
      keyRangeList.add(keyRange);
    }
    KeyRangeComparator comparator = new KeyRangeComparator();
    Collections.sort(keyRangeList, comparator);
    return keyRangeList;
  }

  @Override
  public ResultParser getResultParser() {
    return new NavResultParser();
  }

  private static class NavResultParser implements ResultParser {

    @Override
    public List<RecordUnit> parse(Result result) {
      List<RecordUnit> recordUnits = new ArrayList<RecordUnit>();
      for (KeyValue kv : result.raw()) {
        byte[] rk = kv.getRow();
        String uid = Bytes.toString(Arrays.copyOfRange(rk, UID_INDEX, rk.length));
        String url = Bytes.toString(kv.getValue());
        if (url.startsWith("http://"))
          url = url.substring(7);
        if (url.startsWith("https://"))
          url = url.substring(8);
        if (url.endsWith("/"))
          url = url.substring(0, url.length() - 1);
        recordUnits.add(new RecordUnit(uid, url, 2));
      }
      return recordUnits;
    }
  }
}
