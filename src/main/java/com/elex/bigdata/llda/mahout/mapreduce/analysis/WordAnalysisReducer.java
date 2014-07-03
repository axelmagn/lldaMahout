package com.elex.bigdata.llda.mahout.mapreduce.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/3/14
 * Time: 5:56 PM
 * To change this template use File | Settings | File Templates.
 */
public class WordAnalysisReducer extends Reducer<Text,IntWritable,Text,Text> {
}
