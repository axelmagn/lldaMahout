package com.elex.bigdata.llda.mahout.util;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/22/14
 * Time: 5:31 PM
 * To change this template use File | Settings | File Templates.
 */
public class Trie {
  private Node root;

  public Trie() {
    root = new Node();
  }

  public void insert(String word,int count) {
    if (search(word)) return;
    Node current = root;
    for (int i = 0; i < word.length(); i++) {
      char letter = word.charAt(i);
      Node child = current.subNode(letter);
      if (child != null) {
        current = child;
      } else {
        Node nextNode = new Node();
        current.nextNodes.put(letter, nextNode);
        current = nextNode;
      }
      current.count+=count;
    }
    current.isEnd = true;
  }
  public void insert(String word){
    if (search(word)) return;
    Node current = root;
    for (int i = 0; i < word.length(); i++) {
      char letter = word.charAt(i);
      Node child = current.subNode(letter);
      if (child != null) {
        current = child;
      } else {
        Node nextNode = new Node();
        current.nextNodes.put(letter, nextNode);
        current = nextNode;
      }
      current.count+=1;
    }
    current.isEnd = true;
  }

  public boolean search(String word) {
    Node current = root;

    for (int i = 0; i < word.length(); i++) {
      char letter = word.charAt(i);
      current = current.subNode(letter);
      if (current == null)
        return false;
    }
    return current.isEnd;
  }


  public void deleteWord(String word) {
    if (!search(word)) return;

    Node current = root;
    for (int i = 0; i < word.length(); i++) {
      char letter = word.charAt(i);
      Node child = current.subNode(letter);
      if (child.count == 1) {
        current.nextNodes.remove(letter);
        return;
      } else {
        child.count--;
        current = child;
      }
    }
  }

  public void deleteWord(String word,int count) {
    if (!search(word)) return;

    Node current = root;
    for (int i = 0; i < word.length(); i++) {
      char letter = word.charAt(i);
      Node child = current.subNode(letter);
      if (child.count == 1) {
        current.nextNodes.remove(letter);
        return;
      } else {
        child.count-=count;
        current = child;
      }
    }
  }


  public Map<String,Integer> searchCommonStr(char seperator) {
    return searchCommonStr(root, "", seperator);
  }

  private Map<String,Integer> searchCommonStr(Node node, String prefix, char seperator) {
    Map<String,Integer> commonStrs = new HashMap<String, Integer>();
    int index;
    if (node.isEnd) {
      commonStrs.put(prefix,node.count);
      return commonStrs;
    } else if (prefix.length() >= 1 && (index=prefix.lastIndexOf('/'))!=-1 && node.nextNodes.size() > 1) {
      int maxCount=0;
      for(Map.Entry<Character,Node> entry: node.nextNodes.entrySet()){
         if(entry.getValue().count>maxCount)
           maxCount=entry.getValue().count;
      }
      commonStrs.put(prefix.substring(0, index),maxCount);
      return commonStrs;
    } else {
      for (Map.Entry<Character, Node> entry : node.nextNodes.entrySet()) {
        for (Map.Entry<String,Integer> wordCount: searchCommonStr(entry.getValue(), prefix + entry.getKey(), seperator).entrySet()) {
          commonStrs.put(wordCount.getKey(),wordCount.getValue());
        }
      }
    }
    return commonStrs;
  }

  public static class Node {
    boolean isEnd;
    int count;  // the number of words sharing this character
    Map<Character, Node> nextNodes;

    public Node() {
      nextNodes = new HashMap<Character, Node>();
      count = 0;
      isEnd = false;
    }

    public Node subNode(char c) {
      return nextNodes.get(c);
    }
  }


}

