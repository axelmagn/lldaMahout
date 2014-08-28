package com.elex.bigdata.llda.mahout.data.accumulateurlcount;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/28/14
 * Time: 11:16 AM
 * To change this template use File | Settings | File Templates.
 */
public class RecordUnit {
  private String word;
  private String uid;
  private int count;
  public RecordUnit(String uid,String word,int count){
    this.word=word;
    this.count=count;
    this.uid=uid;
  }

  public String getWord() {
    return word;
  }

  public void setWord(String word) {
    this.word = word;
  }

  public String getUid() {
    return uid;
  }

  public void setUid(String uid) {
    this.uid = uid;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }
}
