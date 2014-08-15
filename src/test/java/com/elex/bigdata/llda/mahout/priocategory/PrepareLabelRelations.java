package com.elex.bigdata.llda.mahout.priocategory;

import com.elex.bigdata.llda.mahout.priocatogory.ParentToChildLabels;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/15/14
 * Time: 3:27 PM
 * To change this template use File | Settings | File Templates.
 */
public class PrepareLabelRelations {
  @Test
  public void prepareLabelRelations() throws IOException {
    int[] parentLabel = new int[23];
    for (int i = 0; i < parentLabel.length; i++)
      parentLabel[i] = (i + 1);
    int[] childLabels = new int[]{1, 101, 103, 105, 111, 113, 120, 121, 122, 123, 125, 128, 129, 135, 139, 141, 144,145, 146, 147};
    assert childLabels.length == 20;
    List<ParentToChildLabels> parentToChildLabelsList = new ArrayList<ParentToChildLabels>();
    parentToChildLabelsList.add(new ParentToChildLabels(parentLabel[0], childLabels));
    for (int i = 1; i < parentLabel.length; i++) {
      parentToChildLabelsList.add(new ParentToChildLabels(parentLabel[i], new int[]{parentLabel[i]}));
    }
    ObjectMapper objectMapper = new ObjectMapper();
    BufferedWriter writer = new BufferedWriter(new FileWriter("/data/log/user_category/llda/categories/result/labelRelations"));
    for (ParentToChildLabels parentToChildLabels : parentToChildLabelsList) {
      writer.write(objectMapper.writeValueAsString(parentToChildLabels));
      writer.newLine();
    }
    writer.close();
  }
}
