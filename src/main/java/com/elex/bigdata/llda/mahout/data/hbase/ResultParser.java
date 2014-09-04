package com.elex.bigdata.llda.mahout.data.hbase;

import org.apache.hadoop.hbase.client.Result;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/28/14
 * Time: 11:14 AM
 * To change this template use File | Settings | File Templates.
 */
public interface ResultParser {
  List<RecordUnit> parse(Result result);
}
