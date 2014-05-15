package com.elex.bigdata.llda.mahout.data.mergedocs;

import org.apache.mahout.common.AbstractJob;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 10:12 AM
 * To change this template use File | Settings | File Templates.
 */
public class MergeLDocDriver extends AbstractJob {
  /*
     InputPaths: labeledDocs files
     conf.setDictSizePath
     MergeLDocMapper:
        create a new labeledDocument with size of dictSize and clone from value
     MergeLDocReducer:
        merge labels and urlCounts for uid:Iterable<LabeledDocument>
        context.write(uid,labeledDocument)
   */
  @Override
  public int run(String[] strings) throws Exception {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
