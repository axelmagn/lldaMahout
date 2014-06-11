package com.elex.bigdata.llda.mahout.data.accumulateurlcount;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 4/11/14
 * Time: 6:22 PM
 * To change this template use File | Settings | File Templates.
 */
public class PutUrlCount {
  private Map<Path, BufferedWriter> writers = new ConcurrentHashMap<Path, BufferedWriter>();
  private FileSystem fs;

  public PutUrlCount(FileSystem fs) {
    this.fs = fs;
  }

  public PutUrlCountRunnable getRunnable(Path filePath, Map<String, Map<String, Integer>> uidUrlCountMap) throws IOException {
    BufferedWriter writer;
    if (writers.containsKey(filePath))
      writer = writers.get(filePath);
    else {
      FSDataOutputStream outputStream = fs.create(filePath);
      writer = new BufferedWriter(new OutputStreamWriter(outputStream));
      writers.put(filePath, writer);
    }
    System.out.println(filePath.getName() + " uid size " + uidUrlCountMap.size());
    return new PutUrlCountRunnable(writer, uidUrlCountMap);

  }

  public static class PutUrlCountRunnable implements Runnable {
    Writer writer;
    private Map<String, Map<String, Integer>> uidUrlCountMap = new HashMap<String, Map<String, Integer>>();

    public PutUrlCountRunnable(Writer writer, Map<String, Map<String, Integer>> uidUrlCountMap) {
      this.uidUrlCountMap = uidUrlCountMap;
      this.writer = writer;
    }

    @Override
    public void run() {
      try {
        for (Map.Entry<String, Map<String, Integer>> entry : uidUrlCountMap.entrySet()) {
          String uid = entry.getKey();
          Map<String, Integer> urlCount = entry.getValue();

          for (Map.Entry<String, Integer> urlCountEntry : urlCount.entrySet()) {
            StringBuilder builder = new StringBuilder();
            builder.append(uid + "\t");
            builder.append(urlCountEntry.getKey() + "\t" + urlCountEntry.getValue());
            writer.write(builder.toString() + "\r\n");
          }
        }
        writer.flush();
        System.out.println("finished");
      } catch (IOException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }
  }
}
