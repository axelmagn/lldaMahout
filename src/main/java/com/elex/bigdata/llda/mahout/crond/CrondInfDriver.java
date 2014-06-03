package com.elex.bigdata.llda.mahout.crond;

import org.apache.mahout.common.AbstractJob;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/29/14
 * Time: 6:08 PM
 * To change this template use File | Settings | File Templates.
 */
public class CrondInfDriver extends AbstractJob{
  @Override
  public int run(String[] strings) throws Exception {
    /*
      updateDictInput,dictionaryRoot;textInput(orignal format),docsRoot(multiLabelVectorWritable format),docsDir(relative current docs dir)

     */
    addInputOption();
    addOutputOption();

    return 0;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
