package com.elex.bigdata.llda.mahout.dictionary;

import com.elex.bigdata.hashing.BDMD5;
import com.elex.bigdata.hashing.HashingException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger log = LoggerFactory.getLogger(UpdateRegularDictReducer.class);
  private String dictPath;
  private FileSystem fs;
  private List<OpenObjectIntHashMap<String>> dayDicts;
  private OpenObjectIntHashMap<String> dict;
  private OpenObjectIntHashMap<String> latentDict = null;
  private OpenObjectIntHashMap<String> freshDict;
  private Set<String> notHitWords;
  private Integer dictSize;
  private int collisionCount = 0, hitWordCount=0,wordCount=0;
  private boolean loadDict = false, loadDayDict = false;
  private String user, passwd, ip, port;
  private String tableName = "url_map";
  private Statement statement;
  private ExecutorService service = new ThreadPoolExecutor(3, 5, 3600, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(30));

  public RegularDictionray(String dictPath, FileSystem fs) throws IOException, SQLException, ClassNotFoundException {
    this.fs = fs;
    this.dictPath = dictPath;
    dayDicts = new ArrayList<OpenObjectIntHashMap<String>>();
    dict = new OpenObjectIntHashMap<String>();
    latentDict = new OpenObjectIntHashMap<String>();
    freshDict = new OpenObjectIntHashMap<String>();
    notHitWords = new HashSet<String>();
    Properties properties = new Properties();
    properties.load(this.getClass().getResourceAsStream("/mysql.properties"));
    user = properties.getProperty("user");
    passwd = properties.getProperty("passwd");
    ip = properties.getProperty("ip");
    port = properties.getProperty("port");
    initStatement();
  }

  public void loadDict() throws IOException, HashingException {
    if (fs.exists(new Path(dictPath))) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(dictPath))));
      String line;
      line = reader.readLine();
      dictSize = Integer.parseInt(line);
      while ((line = reader.readLine()) != null) {
        String[] urlIdMaps = line.split("\t");
        if (urlIdMaps.length < 2)
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
    } else {
      log.info(dictPath + " not exists");
      dictSize = 0;
    }
    loadDict = true;
  }

  public void loadDayDicts() throws IOException, HashingException {
    if (fs.exists(new Path(dictPath))) {
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
          int id = Integer.parseInt(urlId[1]);
          dict.put(urlId[0], id);
          dayDict.put(urlId[0], id);
        }
        dayDicts.add(dayDict);
      }
      log.info("regular dict size is "+dict.size());
      log.info("dict Size is "+dictSize);
      reader.close();
    } else {
      log.info(dictPath + " not exists");
      dictSize = 0;
      log.info("dictSize is 0");
    }
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

  public Integer getDictSize() {
    return dictSize;
  }

  public void setDictSize(Integer dictSize) {
    this.dictSize = dictSize;
  }

  public void updateWord(String word) throws SQLException {
    wordCount++;
    if (dict.containsKey(word)) {
      hitWordCount++;
      for (OpenObjectIntHashMap<String> dayDict : dayDicts) {
        if (dayDict.containsKey(word)) {
          latentDict.put(word, dayDict.get(word));
          dayDict.removeKey(word);
          break;
        }
      }
    } else {
      notHitWords.add(word);
      if (notHitWords.size() > 5000)
        queryMysql();
    }
  }

  private void queryMysql() {
    service.execute(new QueryWordRunner(notHitWords));
    notHitWords = new HashSet<String>();
  }

  public void updateWords(List<String> words) {
    for (String word : words) {
      wordCount++;
      if (dict.containsKey(word)) {
        hitWordCount++;
        for (OpenObjectIntHashMap<String> dayDict : dayDicts) {
          if (dayDict.containsKey(word)) {
            latentDict.put(word, dayDict.get(word));
            dayDict.removeKey(word);
            break;
          }
        }
      } else {
        notHitWords.add(word);
        if (notHitWords.size() > 5000)
          queryMysql();
      }
    }
  }

  public void flushToMysql() throws ClassNotFoundException, SQLException, InterruptedException {
    queryMysql();
    service.shutdown();
    service.awaitTermination(30, TimeUnit.MINUTES);
    log.info("hitCount is "+hitWordCount);
    log.info("total word count is "+wordCount);
    log.info("fresh dict size is " + freshDict.size());
    if (freshDict.size() == 0)
      return;
    StringBuilder sql = new StringBuilder();
    sql.append("insert into " + tableName + " values ");
    for (String word : freshDict.keys()) {
      sql.append("('" + word + "'," + freshDict.get(word) + "),");
    }
    sql.deleteCharAt(sql.length() - 1);
    String sqlStr = sql.toString();
    log.info("flush to mysql " + sqlStr);
    statement.execute(sqlStr);

  }

  private void initStatement() throws ClassNotFoundException, SQLException {
    Class.forName("com.mysql.jdbc.Driver");
    String url = "jdbc:mysql://" + ip + ":" + port + "/bigdata";
    log.info(url);
    Connection connectMySQL = DriverManager.getConnection(url, user, passwd);
    log.info(user + ":" + passwd);
    statement = connectMySQL.createStatement();
  }

  public void flushDict() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
    flushToMysql();
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(dictPath))));
    writer.write(String.valueOf(dictSize));
    log.info("when flush dict dictSize is " + dictSize);
    log.info("collision count is " + collisionCount);

    writer.newLine();
    for (String word : freshDict.keys()) {
      writer.write(word + " " + freshDict.get(word) + "\t");
    }
    if (dayDicts.size() >= 10)
      dayDicts.remove(dayDicts.size() - 1);
    for (OpenObjectIntHashMap<String> dayDict : dayDicts) {
      for (String word : dayDict.keys()) {
        writer.write(word + " " + dayDict.get(word) + "\t");
      }
      writer.newLine();
    }
    writer.flush();
  }

  private class QueryWordRunner implements Runnable {
    private Set<String> words;

    public QueryWordRunner(Set<String> words) {
      this.words = words;
    }

    @Override
    public void run() {
      StringBuilder querySql = new StringBuilder();
      if (words.size() == 0)
        return;
      querySql.append("select url,id from " + tableName + " where ");
      for (String word : words) {
        querySql.append("url = '" + word + "' or ");
      }
      querySql.delete(querySql.lastIndexOf("or"), querySql.length());
      querySql.append(";");
      try {
        String querySqlStr = querySql.toString();
        log.info("query sql :" + querySqlStr);
        ResultSet resultSet = statement.executeQuery(querySqlStr);
        if (resultSet != null)
          while (resultSet.next()) {
            String word = resultSet.getString("url");
            int id = resultSet.getInt("id");
            latentDict.put(word, id);
            words.remove(word);
          }
      } catch (SQLException e) {
        log.warn(e.getMessage());
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
      for (String word : words) {
        synchronized (dictSize) {
          if (!freshDict.containsKey(word))
            freshDict.put(word, dictSize++);
          else
            collisionCount++;
        }
      }
    }
  }
}
