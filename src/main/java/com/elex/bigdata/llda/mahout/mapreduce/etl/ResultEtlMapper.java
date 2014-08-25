package com.elex.bigdata.llda.mahout.mapreduce.etl;

import com.elex.bigdata.llda.mahout.data.generatedocs.GenerateLDocDriver;
import com.elex.bigdata.llda.mahout.priocatogory.ParentToChildLabels;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.Pair;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;


/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 5/28/14
 * Time: 11:18 AM
 * To change this template use File | Settings | File Templates.
 */
public class ResultEtlMapper extends Mapper<Object, Text, Text, Text> {
  Map<Integer, Integer> child2ParentLabels;
  Map<Integer, int[]> parent2ChildLabels;
  Set<Integer> destParentLabels;

  public void setup(Context context) throws IOException {
    Pair<Map<Integer, Integer>, Map<Integer, int[]>> pair = getLabelRelations(context.getConfiguration());
    child2ParentLabels = pair.getFirst();
    parent2ChildLabels = pair.getSecond();
    destParentLabels = getDestParentLabels();
  }

  private Set<Integer> getDestParentLabels() throws IOException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(
      this.getClass().getResourceAsStream("/" + GenerateLDocDriver.DEST_PARENT_LABELS)));
    Set<Integer> labels = new HashSet<Integer>();
    String line;
    while ((line = reader.readLine()) != null) {
      labels.add(Integer.parseInt(line.trim()));
    }
    return labels;
  }

  private Pair<Map<Integer, Integer>, Map<Integer, int[]>> getLabelRelations(Configuration conf) throws IOException {
    Map<Integer, Integer> child2ParentLabelMap = new HashMap<Integer, Integer>();
    Map<Integer, int[]> parent2ChildMap = new HashMap<Integer, int[]>();
    FileSystem fs = FileSystem.get(conf);
    Path resourcesPath = new Path(conf.get(GenerateLDocDriver.RESOURCE_ROOT));
    Path labelRelationPath = new Path(resourcesPath, ResultEtlDriver.LABEL_RELATION);
    BufferedReader urlCategoryReader = new BufferedReader(new InputStreamReader(fs.open(labelRelationPath)));
    String line;
    ObjectMapper objectMapper = new ObjectMapper();
    while ((line = urlCategoryReader.readLine()) != null) {
      ParentToChildLabels parentToChildLabels = objectMapper.readValue(line.trim(), ParentToChildLabels.class);
      for (Integer label : parentToChildLabels.getChildLabels()) {
        child2ParentLabelMap.put(label, parentToChildLabels.getParentLabel());
      }
      parent2ChildMap.put(parentToChildLabels.getParentLabel(), parentToChildLabels.getChildLabels());
    }
    return new Pair<Map<Integer, Integer>, Map<Integer, int[]>>(child2ParentLabelMap, parent2ChildMap);
  }

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    Map<Integer, Double> labelProbMap = parseResultLine(value.toString());

    Map<Integer, Double> parentLabelProbMap = new HashMap<Integer, Double>();
    for (Map.Entry<Integer, Double> entry : labelProbMap.entrySet()) {
      Integer parentLabel = child2ParentLabels.get(entry.getKey());
      Double probValue = parentLabelProbMap.get(parentLabel);
      if (probValue == null)
        probValue = new Double(0.0);
      probValue += entry.getValue();
      parentLabelProbMap.put(parentLabel, probValue);
    }

    Integer maxProbParentLabel = 0;
    Double maxProbValue = new Double(0.0);
    for (Map.Entry<Integer, Double> entry : parentLabelProbMap.entrySet()) {
      if (entry.getValue() > maxProbValue) {
        maxProbParentLabel = entry.getKey();
        maxProbValue = entry.getValue();
      }
    }
    String uid = value.toString().split("\t")[0];
    if (!destParentLabels.contains(maxProbParentLabel)) {
      context.write(new Text(uid), new Text(String.valueOf(0)));
      context.write(new Text(uid.toUpperCase()), new Text(String.valueOf(0)));
      return;
    }
    int[] labels = parent2ChildLabels.get(maxProbParentLabel);
    int maxProbChildLabel = 0;
    maxProbValue = new Double(0.0);
    for (int label : labels) {
      Double probValue = labelProbMap.get(label);
      if (probValue > maxProbValue) {
        maxProbChildLabel = label;
        maxProbValue = probValue;
      }
    }
    if (maxProbValue < 1 / ((double) labelProbMap.size()*1.5)) {
      context.write(new Text(uid), new Text(String.valueOf(0)));
      context.write(new Text(uid.toUpperCase()), new Text(String.valueOf(0)));
    } else {
      context.write(new Text(uid), new Text(String.valueOf(maxProbChildLabel)));
      context.write(new Text(uid.toUpperCase()), new Text(String.valueOf(maxProbChildLabel)));
    }
  }

  private Map<Integer, Double> parseResultLine(String resultLine) {
    Map<Integer, Double> labelProbMap = new HashMap<Integer, Double>();
    String[] results = resultLine.split("\t");
    String[] probs = results[results.length - 1].split(",");
    for (String probStr : probs) {
      String[] tokens = probStr.split(":");
      labelProbMap.put(Integer.parseInt(tokens[0]), Double.parseDouble(tokens[1]));
    }
    return labelProbMap;
  }
}
