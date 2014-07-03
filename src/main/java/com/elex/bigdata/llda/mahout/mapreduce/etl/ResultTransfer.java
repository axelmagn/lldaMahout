package com.elex.bigdata.llda.mahout.mapreduce.etl;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/3/14
 * Time: 11:51 AM
 * To change this template use File | Settings | File Templates.
 */
public class ResultTransfer {
  public static void main(String[] args) throws IOException, DecoderException {
    File file=new File(args[0]);
    BufferedReader reader=new BufferedReader(new FileReader(file));
    File tmpFile=new File(args[0]+"_tmp");
    BufferedWriter writer=new BufferedWriter(new FileWriter(tmpFile));
    String line;
    String[] categories=new String[]{"a","b","c","d","z"};
    while((line=reader.readLine())!=null){
       String[] tokens=line.split("\t");
       if(tokens.length<2)
         continue;
       String uid=tokens[0];
       String[] probs=tokens[1].split(";");
       if(probs.length<5)
         continue;
       int maxProbIndex=0;
       byte[] maxProb=Hex.decodeHex(probs[0].split("=")[1].toCharArray());
       for(int i=1;i<probs.length-1;i++){
          byte[] prob=Hex.decodeHex(probs[i].split("=")[1].toCharArray());
          if(Bytes.compareTo(prob,maxProb)>0)
          {
            maxProbIndex=i;
            maxProb=prob;
          }
       }
       if(Bytes.compareTo(maxProb,Bytes.toBytes(10))<0)
         maxProbIndex=probs.length-1;
       String category=categories[maxProbIndex];
      writer.write(uid+"\t"+category);
      writer.newLine();
    }
    writer.flush();
    writer.close();
    reader.close();
    file.delete();
    tmpFile.renameTo(file);
  }
}
