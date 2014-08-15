package com.elex.bigdata.llda.mahout.priocatogory;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/15/14
 * Time: 3:21 PM
 * To change this template use File | Settings | File Templates.
 */
public class ParentToChildLabels {
  private int parentLabel;
  private int[] childLabels;
  public ParentToChildLabels(@JsonProperty("parent") int parentLabel,@JsonProperty("childs") int[] childLabels){
    this.parentLabel=parentLabel;
    this.childLabels=childLabels;
  }

  public int getParentLabel() {
    return parentLabel;
  }

  public void setParentLabel(int parentLabel) {
    this.parentLabel = parentLabel;
  }

  public int[] getChildLabels() {
    return childLabels;
  }

  public void setChildLabels(int[] childLabels) {
    this.childLabels = childLabels;
  }
}
