package com.elex.bigdata.llda.mahout.data.mergedocs;

import com.elex.bigdata.llda.mahout.data.LabeledDocument;
import com.elex.bigdata.llda.mahout.data.LabeledDocumentWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/14/14
 * Time: 10:28 AM
 * To change this template use File | Settings | File Templates.
 */
public class MergeLDocReducer extends Reducer<Text,LabeledDocumentWritable,Text,LabeledDocumentWritable> {
   public void reduce(Text key,Iterable<LabeledDocumentWritable> values,Context context) throws IOException, InterruptedException {
     /*
        merge multi labeledDocumentWritable and write to file
     */
     List<LabeledDocument> lDocs=new ArrayList<LabeledDocument>();
     for(LabeledDocumentWritable labeledDocumentWritable:values){
       lDocs.add(labeledDocumentWritable.get());
     }
     LabeledDocument finalLDoc=LabeledDocument.mergeDocs(lDocs);
     context.write(key,new LabeledDocumentWritable(finalLDoc));
   }
}
