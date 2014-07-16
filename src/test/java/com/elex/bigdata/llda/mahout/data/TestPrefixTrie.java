package com.elex.bigdata.llda.mahout.data;

import com.elex.bigdata.llda.mahout.util.PrefixTrie;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/11/14
 * Time: 3:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestPrefixTrie {
  @Test
  public void prefixSearch() throws IOException {
    String[] destCategories = new String[]{"jogos", "compras", "Friends", "Tourism"};
    Map<String, Integer> categoryIdMap = new HashMap<String, Integer>();
    Map<Integer, String> idCategoryMap = new HashMap<Integer, String>();
    for (int i = 0; i < destCategories.length; i++) {
      categoryIdMap.put(destCategories[i], i);
      idCategoryMap.put(i, destCategories[i]);
    }
    PrefixTrie prefixTrie = new PrefixTrie();

    Map<String, String> url_category_map = new HashMap<String, String>();
    InputStream inputStream = this.getClass().getResourceAsStream("/url_category");
    BufferedReader urlCategoryReader = new BufferedReader(new InputStreamReader(inputStream));
    String line = "";
    while ((line = urlCategoryReader.readLine()) != null) {
      String[] categoryUrls = line.split(" ");
      if (categoryIdMap.containsKey(categoryUrls[0])) {
        int id = categoryIdMap.get(categoryUrls[0]);
        for (int i = 1; i < categoryUrls.length; i++)
          prefixTrie.insert(categoryUrls[i], id);
      } else {
        for (int i = 1; i < categoryUrls.length; i++) {
          url_category_map.put(categoryUrls[i], categoryUrls[0]);
        }
      }
    }
    urlCategoryReader.close();
    String[] urls = new String[]{"www.jogos.com", "www.neoseeker.com/Games/Products/PSX/legend_dragoon/legend_dragoon_cheats.html", "songbird-productions.com/protector_se.shtml", "www.game-over.net/review/october/caesar3/index.html", "www.freewebs.com/mtanl",
      "www.renewalresearch.com", "www.entowinkler.at", "www.officemax.com", "backyardscoreboards.com", "www.williswinebar.us", "www.unifold.net", "www.drumsonsale.com", "www.sportingkicks.co.uk", "www.affordabledesigns.net"
    };
    /*
    prefixTrie.insert("www.compras.com");
    prefixTrie.insert("www.jogos.com/hello.html");
    prefixTrie.insert("www.cup.com");
    for (String url: urls){
      prefixTrie.insert(url);
    }
    */
    String[] destUrls = new String[]{"www.jogos.com", "www.jogos.com/myword", "www.neoseeker.com", "www.jogos.cn/myword", "backyardscoreboards.com/hello?myapp=12&myid=15"};
    long t1 = System.currentTimeMillis(),startTime=System.nanoTime();
    for (int i = 0; i < 1000 * 1000*10; i++) {
      for (String url : destUrls) {
        String category = url_category_map.get(url.toString());
        if (category == null) {
          int id = prefixTrie.prefixSearch(url.toString());
          if (id != -1)
            category = idCategoryMap.get(id);
        }
      }
    }
    long t2 = System.currentTimeMillis();
    System.out.println((t2 - t1) + " ms");
    System.out.println((System.nanoTime()-startTime)/(1000*1000)+" ms");

  }

  @Test
  public void testHash() {
    long t1 = System.nanoTime();
    new String("ccc").hashCode();
    System.out.println(System.nanoTime() - t1);
    String str=new String("www.jogos.com/hello.htmldddddddddddddddddddddddddddccccccccccccccddsssssssssssdfgfdssssssss");
    Character c=new Character('b');
    for (int i = 0; i < 10; i++) {
      t1 = System.nanoTime();
      str.hashCode();
      System.out.println((System.nanoTime() - t1) + " :b");
      t1 = System.nanoTime();
      c.hashCode();
      System.out.println((System.nanoTime() - t1)+" C");
    }
    t1 = System.nanoTime();
    long t2=System.currentTimeMillis();
    new String("www.jogos.com").hashCode();
    System.out.println(System.nanoTime() - t1);
    System.out.println(System.currentTimeMillis()-t2);
  }
}
