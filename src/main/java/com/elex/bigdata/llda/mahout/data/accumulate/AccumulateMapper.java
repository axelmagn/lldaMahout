package com.elex.bigdata.llda.mahout.data.accumulate;

import com.elex.bigdata.llda.mahout.data.hbase.RecordUnit;
import com.elex.bigdata.llda.mahout.data.hbase.ResultParser;
import com.elex.bigdata.llda.mahout.data.hbase.SuperTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/28/14
 * Time: 2:19 PM
 * To change this template use File | Settings | File Templates.
 */
public class AccumulateMapper extends TableMapper<Text,Text>{
  // to parse Result
  private ResultParser resultParser;
  // to save recordUnit_count
  private Map<RecordUnit,Integer> recordUnit2Count =new HashMap<RecordUnit, Integer>();
  private int num=0;
  public void setup(Context context){
    Configuration conf=context.getConfiguration();
    String tableType=conf.get(Accumulate.TABLE_TYPE);
    try {
      Class tableTypeClass=Class.forName(tableType);
      SuperTable typeTable=(SuperTable)tableTypeClass.newInstance();
      resultParser=typeTable.getResultParser();
      System.out.println("tableType "+tableType);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (InstantiationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (IllegalAccessException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
  public void map(ImmutableBytesWritable row,Result result,Context context) throws IOException, InterruptedException {
    num++;
    List<RecordUnit> recordUnits =resultParser.parse(result);
    for(RecordUnit recordUnit : recordUnits){
      if(recordUnit2Count.containsKey(recordUnit))
        recordUnit2Count.put(recordUnit, recordUnit2Count.get(recordUnit)+ 1);
      else
        recordUnit2Count.put(recordUnit, 1);
    }

    if(num%50000==0){
      for(Map.Entry<RecordUnit,Integer> entry: recordUnit2Count.entrySet()){
        context.write(new Text(entry.getKey().toString()),new Text(String.valueOf(entry.getValue())));
      }
      num=0;
      recordUnit2Count =new HashMap<RecordUnit, Integer>();
    }
  }

  public void cleanup(Context context) throws IOException, InterruptedException {
    for(Map.Entry<RecordUnit,Integer> entry: recordUnit2Count.entrySet()){
      context.write(new Text(entry.getKey().toString()),new Text(String.valueOf(entry.getValue())));
    }
  }

}
