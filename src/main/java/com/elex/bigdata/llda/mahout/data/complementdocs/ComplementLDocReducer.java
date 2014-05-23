package com.elex.bigdata.llda.mahout.data.complementdocs;

import com.elex.bigdata.llda.mahout.data.LabeledDocument;
import com.elex.bigdata.llda.mahout.data.LabeledDocumentWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.MultiLabelVectorWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 11:27 AM
 * To change this template use File | Settings | File Templates.
 */
public class ComplementLDocReducer extends Reducer<Text,MultiLabelVectorWritable,Text,MultiLabelVectorWritable> {
  public void reduce(Text key,Iterable<MultiLabelVectorWritable> values, Context context) throws IOException, InterruptedException {
     /*
         merge values
         context.write(key,value)
     */
    List<MultiLabelVectorWritable> lDocs=new ArrayList<MultiLabelVectorWritable>();
    for(MultiLabelVectorWritable labelVectorWritable: values){
      lDocs.add(labelVectorWritable);
    }
    if(lDocs.size()==1)
      context.write(key,lDocs.get(0));
    MultiLabelVectorWritable finalLDoc=LabeledDocument.mergeDocs(lDocs);
    context.write(key,finalLDoc);
  }
}
