package com.elex.bigdata.llda.mahout.dictionary;

import com.elex.bigdata.hashing.BDMD5;
import com.elex.bigdata.hashing.HashingException;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
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

  private List<Map<String, Integer>> dayDicts;
  private Map<String, Integer> dict;
  private Map<String, Integer> latentDict = null;
  private Map<String, Integer> freshDict;
  private Set<String> notHitWords;

  private Integer dictSize;
  private int collisionCount = 0, hitWordCount = 0, wordCount = 0;
  private boolean loadDict = false, loadDayDict = false;
  private int queryBatch=20000,updateBatch=20000;
  private BasicDataSource dataSource;
  private String tableName = "url_map";
  private ExecutorService queryService = new ThreadPoolExecutor(3, 8, 3600, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(30));
  private ExecutorService updateService = new ThreadPoolExecutor(3, 8, 3600, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(30));

  public RegularDictionray(String dictPath, FileSystem fs) throws IOException, SQLException, ClassNotFoundException {
    this.fs = fs;
    this.dictPath = dictPath;
    dayDicts = new ArrayList<Map<String, Integer>>();
    dict = new HashMap<String, Integer>();
    latentDict = new HashMap<String, Integer>();
    freshDict = new HashMap<String, Integer>();
    notHitWords = new HashSet<String>();
    initDataSource();


  }

  private void initDataSource() throws IOException {
    Properties properties = new Properties();
    properties.load(this.getClass().getResourceAsStream("/mysql.properties"));
    dataSource = new BasicDataSource();
    dataSource.setDriverClassName("com.mysql.jdbc.Driver");
    String host = properties.getProperty("ip");
    String port = properties.getProperty("port");
    String db = properties.getProperty("db");
    String user = properties.getProperty("user");
    String passwd = properties.getProperty("passwd");
    int maxActive = Integer.parseInt(properties.getProperty("max_active"));
    int maxIdle = Integer.parseInt(properties.getProperty("max_idle"));
    int initConn = Integer.parseInt(properties.getProperty("init_conn"));
    int evicIdleTime = Integer.parseInt(properties.getProperty("evictable_idle_time"));
    String url = "jdbc:mysql://"
      + host
      + ":"
      + port
      + "/"
      + db
      + "?autoReconnect=true&useUnicode=true&characterEncoding=utf-8";
    dataSource.setUrl(url);
    dataSource.setUsername(user);
    dataSource.setPassword(passwd);
    dataSource.setInitialSize(initConn);
    dataSource.setMaxActive(maxActive);
    dataSource.setMaxIdle(maxIdle);
    dataSource.setMinEvictableIdleTimeMillis(evicIdleTime);

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
        Map<String, Integer> dayDict = new HashMap<String, Integer>();
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
      log.info("regular dict size is " + dict.size());
      log.info("dict Size is " + dictSize);
      reader.close();
    } else {
      log.info(dictPath + " not exists");
      dictSize = 0;
      log.info("dictSize is 0");
    }
    loadDayDict = true;
    loadDict = true;
  }

  public Map<String, Integer> getDict() throws IOException, HashingException {
    if (!loadDict)
      loadDict();
    return dict;
  }

  public List<Map<String, Integer>> getDayDicts() throws IOException, HashingException {
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
      for (Map<String, Integer> dayDict : dayDicts) {
        if (dayDict.containsKey(word)) {
          latentDict.put(word, dayDict.get(word));
          dayDict.remove(word);
          break;
        }
      }
    } else {
      notHitWords.add(word);
      if (notHitWords.size() > 20000)
        queryMysql();
    }
  }

  private void queryMysql() {
    queryService.execute(new QueryWordRunner(notHitWords));
    notHitWords = new HashSet<String>();
  }

  public void updateWords(List<String> words) {
    for (String word : words) {
      wordCount++;
      if (dict.containsKey(word)) {
        hitWordCount++;
        for (Map<String, Integer> dayDict : dayDicts) {
          if (dayDict.containsKey(word)) {
            latentDict.put(word, dayDict.get(word));
            dayDict.remove(word);
            break;
          }
        }
      } else {
        notHitWords.add(word);
        if (notHitWords.size() > 20000)
          queryMysql();
      }
    }
  }

  public void flushToMysql() throws ClassNotFoundException, SQLException, InterruptedException {

    if (freshDict.size() == 0)
      return;
    StringBuilder sqlBuilder = new StringBuilder();
    String insertPrefix="insert into " + tableName + " values ";
    sqlBuilder.append(insertPrefix);
    int valueSize=0;
    for (Map.Entry<String, Integer> entry : freshDict.entrySet()) {
      valueSize++;
      sqlBuilder.append("('" + entry.getKey() + "'," + entry.getValue() + "),");
      if(valueSize>=updateBatch){
        sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
        String sql=sqlBuilder.toString();
        updateService.execute(new UpdateWordRunner(sql));
        sqlBuilder=new StringBuilder().append(insertPrefix);
        valueSize=0;
      }
    }
    if(valueSize>0){
      String sqlStr = sqlBuilder.toString();
      updateService.execute(new UpdateWordRunner(sqlStr));
    }
    updateService.shutdown();
    updateService.awaitTermination(30,TimeUnit.MINUTES);
  }


  public void flushDict() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
    {
      queryMysql();
      log.info("service shutdown....");
      queryService.shutdown();
      queryService.awaitTermination(30, TimeUnit.MINUTES);
      log.info("hitCount is " + hitWordCount);
      log.info("total word count is " + wordCount);
      log.info("fresh dict size is " + freshDict.size());
    }
    flushToMysql();
    {
      BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(dictPath))));
      writer.write(String.valueOf(dictSize));
      log.info("when flush dict dictSize is " + dictSize);
      log.info("collision count is " + collisionCount);
      log.info("latent dict size is " + latentDict.size());

      writer.newLine();
      for (Map.Entry<String, Integer> entry : freshDict.entrySet()) {
        writer.write(entry.getKey() + " " + entry.getValue() + "\t");
      }
      if (dayDicts.size() >= 10)
        dayDicts.remove(dayDicts.size() - 1);
      for (Map.Entry<String, Integer> entry : latentDict.entrySet()) {
        writer.write(entry.getKey() + " " + entry.getValue() + "\t");
      }
      writer.newLine();
      for (Map<String, Integer> dayDict : dayDicts) {
        for (Map.Entry<String, Integer> entry : dayDict.entrySet()) {
          writer.write(entry.getKey() + " " + entry.getValue() + "\t");
        }
        writer.newLine();
      }
      writer.flush();
    }
  }

  private class UpdateWordRunner implements Runnable {
    private String sql;
    private Connection connection;

    public UpdateWordRunner(String sql) {
      this.sql = sql;
      try {
        this.connection = dataSource.getConnection();
      } catch (SQLException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }

    @Override
    public void run() {
      try {
        log.info("update sql is "+sql);
        Statement statement = connection.createStatement();
        statement.execute(sql);
        statement.close();
        //To change body of implemented methods use File | Settings | File Templates.
      } catch (SQLException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      } finally {
        try {
          connection.close();
        } catch (SQLException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
      }
    }
  }

  private class QueryWordRunner implements Runnable {
    private Set<String> words;
    private Connection connection;
    public QueryWordRunner(Set<String> words) {
      this.words = words;
      try {
        this.connection=dataSource.getConnection();
      } catch (SQLException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
    }

    @Override
    public void run() {
      if (words.size() == 0)
        return;
      try {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("select url,id from " + tableName + " where ");
        for (String word : words) {
          sqlBuilder.append("url = '" + word + "' or ");
        }
        sqlBuilder.delete(sqlBuilder.lastIndexOf("or"), sqlBuilder.length());
        sqlBuilder.append(";");
        String querySqlStr = sqlBuilder.toString();
        log.info("query sql :" + querySqlStr);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(querySqlStr);
        while (resultSet.next()) {
          String word = resultSet.getString("url");
          int id = resultSet.getInt("id");
          latentDict.put(word, id);
          words.remove(word);
        }
        statement.close();
        log.info("query complete ");
      } catch (SQLException e) {
        log.warn(e.getMessage());
        e.printStackTrace();
      } finally {
        try {
          connection.close();
        } catch (SQLException e) {
          e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
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
