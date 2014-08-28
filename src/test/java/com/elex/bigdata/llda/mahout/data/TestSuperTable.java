package com.elex.bigdata.llda.mahout.data;

import com.elex.bigdata.llda.mahout.data.accumulateurlcount.tables.AdTable;
import com.elex.bigdata.llda.mahout.data.accumulateurlcount.tables.SuperTable;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/28/14
 * Time: 3:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestSuperTable {
  @Test
  public void testClassName() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    SuperTable tableType=new AdTable();
    System.out.println(tableType.getClass().toString());
    System.out.println(tableType.getClass().getName());
    Class tableTypeClass=Class.forName(tableType.getClass().getName());
    Object object = (SuperTable)tableTypeClass.newInstance();
    System.out.println(object.getClass().getName());
  }
}
