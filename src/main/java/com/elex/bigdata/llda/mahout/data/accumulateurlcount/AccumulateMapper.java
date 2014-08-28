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
  private ResultParser resultParser;
  private Map<UidWord,Integer> uidWord2Count=new HashMap<UidWord, Integer>();
  private int num=0;
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
    num++;
    List<RecordUnit> recordUnits=resultParser.parse(result);
    for(RecordUnit recordUnit: recordUnits){
      UidWord uidWord=new UidWord(recordUnit.getUid(),recordUnit.getWord());
      if(uidWord2Count.containsKey(uidWord))
        uidWord2Count.put(uidWord,uidWord2Count.get(uidWord)+recordUnit.getCount());
      else
        uidWord2Count.put(uidWord,recordUnit.getCount());
    }
    if(num%50000==0){
      for(Map.Entry<UidWord,Integer> entry: uidWord2Count.entrySet()){
        context.write(new Text(entry.getKey().toString()),new Text(String.valueOf(entry.getValue())));
      }
      num=0;
      uidWord2Count=new HashMap<UidWord, Integer>();
    }
  }

  public void cleanup(Context context) throws IOException, InterruptedException {
    for(Map.Entry<UidWord,Integer> entry: uidWord2Count.entrySet()){
      context.write(new Text(entry.getKey().toString()),new Text(String.valueOf(entry.getValue())));
    }
  }

  private static class UidWord{
    private String uid;
    private String word;
    public UidWord(String uid,String word){
      this.uid=uid;
      this.word=word;
    }

    private String getUid() {
      return uid;
    }

    private void setUid(String uid) {
      this.uid = uid;
    }

    private String getWord() {
      return word;
    }

    private void setWord(String word) {
      this.word = word;
    }

    public boolean equals(Object other){
      if(!(other instanceof UidWord))
        return false;
      UidWord otherUidWord=(UidWord)other;
      return (uid.equals(otherUidWord.getUid())&&word.equals(otherUidWord.getWord()));
    }
    public String toString(){
      return uid+"\t"+word;
    }
  }
}
