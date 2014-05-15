package com.elex.bigdata.llda.mahout.data.complementdocs;

import org.apache.mahout.common.AbstractJob;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 10:57 AM
 * To change this template use File | Settings | File Templates.
 */
public class ComplementLDocDriver extends AbstractJob {
  /*
      leftInputPaths: lDocs to complement the docs
      rightInputPath: lDocs which will be complemented
      rightKeyPath; uidFile
      OutputPath: completeLDocs
      ComplementLDocMapper:
         extract uid from uidFile;
         write uid and labeledDocument to reducer (uid in uidFile)
      ComplementLDocReducer:
         merge labeledDocuments with same uid
         write them to hdfs
  */
  @Override
  public int run(String[] strings) throws Exception {
    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
