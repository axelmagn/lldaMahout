package com.elex.bigdata.llda.mahout.data.accumulateurlcount;

import com.elex.bigdata.llda.mahout.data.accumulateurlcount.tables.ResultParser;
import com.elex.bigdata.llda.mahout.data.accumulateurlcount.tables.SuperTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.List;


/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/28/14
 * Time: 2:19 PM
 * To change this template use File | Settings | File Templates.
 */
public class AccumulateMapper extends TableMapper<Text,IntWritable>{
  private ResultParser resultParser;
  public void setup(Context context){
    Configuration conf=context.getConfiguration();
    String tableType=conf.get(Accumulate.TABLE_TYPE);
    try {
      Class tableTypeClass=Class.forName(tableType);
      SuperTable typeTable=(SuperTable)tableTypeClass.newInstance();
      resultParser=typeTable.getResultParser();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (InstantiationException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (IllegalAccessException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
  }
  public void map(ImmutableBytesWritable row,Result result,Context context) throws IOException, InterruptedException {
    List<RecordUnit> recordUnits=resultParser.parse(result);
    for(RecordUnit recordUnit: recordUnits){
      context.write(new Text(recordUnit.getUid()+"\t"+recordUnit.getWord()),new IntWritable(recordUnit.getCount()));
    }
  }
}
