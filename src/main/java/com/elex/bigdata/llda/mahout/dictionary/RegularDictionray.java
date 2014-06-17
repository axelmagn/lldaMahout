package com.elex.bigdata.llda.mahout.dictionary;

import com.elex.bigdata.hashing.BDMD5;
import com.elex.bigdata.hashing.HashingException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.map.OpenObjectIntHashMap;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 6/17/14
 * Time: 3:19 PM
 * To change this template use File | Settings | File Templates.
 */
public class RegularDictionray {

  private String dictPath;
  private FileSystem fs;
  private List<OpenObjectIntHashMap<String>> dayDicts;
  private OpenObjectIntHashMap<String> dict;
  private OpenObjectIntHashMap<String> latentDict = null;
  private OpenObjectIntHashMap<String> freshDict;
  private Set<String> notHitWords;
  private Integer dictSize;
  private boolean loadDict = false, loadDayDict = false;
  private String user,passwd,ip,port;
  private String tableName="url_map";
  private Statement statement;
  private ExecutorService service=new ThreadPoolExecutor(3,5,3600, TimeUnit.SECONDS,new ArrayBlockingQueue<Runnable>(30));

  public RegularDictionray(String dictPath, FileSystem fs) throws IOException, SQLException, ClassNotFoundException {
    this.fs = fs;
    this.dictPath = dictPath;
    dayDicts = new ArrayList<OpenObjectIntHashMap<String>>();
    dict = new OpenObjectIntHashMap<String>();
    latentDict = new OpenObjectIntHashMap<String>();
    freshDict = new OpenObjectIntHashMap<String>();
    notHitWords=new HashSet<String>();
    Properties properties=new Properties();
    properties.load(this.getClass().getResourceAsStream("/mysql.properties"));
    user=properties.getProperty("user");
    passwd=properties.getProperty("passwd");
    ip=properties.getProperty("ip");
    port=properties.getProperty("port");
    initStatement();
  }

  public void loadDict() throws IOException, HashingException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(dictPath))));
    String line;
    line = reader.readLine();
    dictSize = Integer.parseInt(line);
    while ((line = reader.readLine()) != null) {
      String[] urlIdMaps = line.split("\t");
      if(urlIdMaps.length<2)
        continue;
      for (String urlIdMap : urlIdMaps) {
        String[] urlId = urlIdMap.split(" ");
        if (urlId.length < 2)
          continue;
        String urlMd5 = BDMD5.getInstance().toMD5(urlId[0]);
        int id = Integer.parseInt(urlId[1]);
        dict.put(urlMd5, id);
      }
    }
    reader.close();
    loadDict = true;
  }

  public void loadDayDicts() throws IOException, HashingException {
    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(dictPath))));
    String line;
    line = reader.readLine();
    dictSize = Integer.parseInt(line);
    while ((line = reader.readLine()) != null) {
      String[] urlIdMaps = line.split("\t");
      OpenObjectIntHashMap<String> dayDict = new OpenObjectIntHashMap<String>();
      for (String urlIdMap : urlIdMaps) {
        String[] urlId = urlIdMap.split(" ");
        if (urlId.length < 2)
          continue;
        String urlMd5 = BDMD5.getInstance().toMD5(urlId[0]);
        int id = Integer.parseInt(urlId[1]);
        dict.put(urlMd5, id);
        dayDict.put(urlMd5, id);
      }
      dayDicts.add(dayDict);
    }
    reader.close();
    loadDayDict = true;
    loadDict = true;
  }

  public OpenObjectIntHashMap<String> getDict() throws IOException, HashingException {
    if (!loadDict)
      loadDict();
    return dict;
  }

  public List<OpenObjectIntHashMap<String>> getDayDicts() throws IOException, HashingException {
    if (!loadDayDict)
      loadDayDicts();
    return dayDicts;
  }

  public int getDictSize() {
    return dictSize;
  }

  public void setDictSize(int dictSize) {
    this.dictSize = dictSize;
  }

  public void updateWord(String word) throws SQLException {
    if (dict.containsKey(word)) {
      for (OpenObjectIntHashMap<String> dayDict : dayDicts) {
        if (dayDict.containsKey(word)) {
          latentDict.put(word, dayDict.get(word));
          dayDict.removeKey(word);
          break;
        }
      }
    } else {
      notHitWords.add(word);
      if(notHitWords.size()>5000)
        queryMysql();
    }
  }

  private void queryMysql(){
     service.execute(new QueryWordRunner(notHitWords));
     notHitWords=new HashSet<String>();
  }

  public void updateWords(List<String> words) {
    for (String word : words) {
      if (dict.containsKey(word)) {
        for (OpenObjectIntHashMap<String> dayDict : dayDicts) {
          if (dayDict.containsKey(word)) {
            latentDict.put(word, dayDict.get(word));
            dayDict.removeKey(word);
            break;
          }
        }
      } else {
        notHitWords.add(word);
        if(notHitWords.size()>5000)
          queryMysql();
      }
    }
  }

  public void flushToMysql() throws ClassNotFoundException, SQLException {
    queryMysql();
    StringBuilder sql=new StringBuilder();
    sql.append("insert into table "+tableName+" values ");
    for(String word: freshDict.keys()){
       sql.append("("+word+","+freshDict.get(word)+"),");
    }
    sql.deleteCharAt(sql.length()-1);
    String sqlStr=sql.toString();
    System.out.println("flush to mysql "+sqlStr);
    statement.execute(sqlStr);

  }

  private void initStatement() throws ClassNotFoundException, SQLException {
    Class.forName("com.mysql.jdbc.Driver");
    String url="jdbc:mysql://"+ip+":"+port+"/bigdata";
    System.out.println(url);
    Connection connectMySQL  =  DriverManager.getConnection(url, user, passwd);
    System.out.println(user+":"+passwd);
    statement =connectMySQL.createStatement();
  }
  public void flushDict() throws SQLException, ClassNotFoundException, IOException {
    flushToMysql();
    BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(fs.create(new Path(dictPath))));
    writer.write(dictSize);
    writer.newLine();
    for(String word:freshDict.keys()){
      writer.write(word+" "+freshDict.get(word)+"\t");
    }
    dayDicts.remove(dayDicts.size()-1);
    for(OpenObjectIntHashMap<String> dayDict: dayDicts){
      for(String word: dayDict.keys()){
        writer.write(word+" "+dayDict.get(word)+"\t");
      }
      writer.newLine();
    }
    writer.flush();
  }

  private class QueryWordRunner implements Runnable{
    private Set<String> words;
    public QueryWordRunner(Set<String> words){
      this.words=words;
    }
    @Override
    public void run() {
       StringBuilder querySql=new StringBuilder();
       querySql.append("select url,id from "+tableName+" where ");
       for(String word: words){
         querySql.append("url="+word+" or ");
       }
       querySql.delete(querySql.lastIndexOf("or"),querySql.length());
       querySql.append(";");
      try {
        String querySqlStr=querySql.toString();
        System.out.println("query sql :"+ querySqlStr);
        ResultSet resultSet=statement.executeQuery(querySqlStr);
        while(resultSet.next()){
          String word=resultSet.getString("url");
          int id=resultSet.getInt("id");
          latentDict.put(word,id);
          words.remove(word);
        }
      } catch (SQLException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
        for(String word: words){
          synchronized (dictSize){
             freshDict.put(word,dictSize++);
          }
        }
    }
  }
}
