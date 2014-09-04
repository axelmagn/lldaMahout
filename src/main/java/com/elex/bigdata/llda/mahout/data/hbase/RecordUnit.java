package com.elex.bigdata.llda.mahout.data.hbase;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/28/14
 * Time: 11:16 AM
 * To change this template use File | Settings | File Templates.
 */
public class RecordUnit {
  private String field;
  private String uid;
  public RecordUnit(){

  }
  public RecordUnit(String uid, String field){
    this.field = field;
    this.uid=uid;
  }

  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
  }

  public String getUid() {
    return uid;
  }

  public void setUid(String uid) {
    this.uid = uid;
  }

  public boolean equals(Object other){
    if(!(other instanceof RecordUnit))
      return false;
    RecordUnit otherUidWord=(RecordUnit)other;
    return (uid.equals(otherUidWord.getUid())&&field.equals(otherUidWord.getField()));
  }
  public String toString(){
    return uid+"\t"+field;
  }
}
