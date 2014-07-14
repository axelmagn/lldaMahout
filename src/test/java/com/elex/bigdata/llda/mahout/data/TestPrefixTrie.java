package com.elex.bigdata.llda.mahout.data;

import com.elex.bigdata.llda.mahout.util.PrefixTrie;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/11/14
 * Time: 3:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestPrefixTrie {
  @Test
  public void prefixSearch(){
    PrefixTrie prefixTrie=new PrefixTrie();
    prefixTrie.insert("www.jogos.com");
    prefixTrie.insert("www.compras.com");
    prefixTrie.insert("www.jogos.com/hello.html");
    prefixTrie.insert("www.cup.com");
    System.out.println(prefixTrie.prefixSearch("www.jogos.com/myword"));
    System.out.println(prefixTrie.prefixSearch("www.jogos.cn/myword"));

  }
}
