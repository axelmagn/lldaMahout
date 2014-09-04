package com.elex.bigdata.llda.mahout.data;


import com.elex.bigdata.llda.mahout.data.hbase.SuperTable;
import com.mongodb.util.Hash;
import com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl;
import org.apache.mahout.clustering.minhash.HashFactory;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/28/14
 * Time: 3:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestSuperTable {
  @Test
  public void loadTableTypeConfig() throws ParserConfigurationException, IOException, SAXException, ClassNotFoundException, IllegalAccessException, InstantiationException {
    String configFile="/table_type.xml";
    Map<String,SuperTable> table2Type=new HashMap<String,SuperTable>();
    URL url=this.getClass().getResource(configFile);
    DocumentBuilderFactory documentBuilderFactory=DocumentBuilderFactory.newInstance();
    DocumentBuilder documentBuilder=documentBuilderFactory.newDocumentBuilder();
    Document doc=documentBuilder.parse(url.getPath());
    Element rootElement=doc.getDocumentElement();
    NodeList nodeList=rootElement.getElementsByTagName("url");
    for(int i=0;i<nodeList.getLength();i++){
      Node node=nodeList.item(i);
      if(node.getNodeType()!=Node.ELEMENT_NODE)
        continue;
      NodeList nodes=node.getChildNodes();
      for(int j=0;j<nodes.getLength();j++){
        Node childNode = nodes.item(j);
        if(childNode.getNodeType()!=Node.ELEMENT_NODE)
          continue;
        Element element=(Element)childNode;
        String tableName=element.getTagName();
        String tableTypeName=element.getTextContent();
        SuperTable superTable=(SuperTable)Class.forName(tableTypeName).newInstance();
        table2Type.put(tableName,superTable);
      }

    }
    System.out.println("hh");
  }
}