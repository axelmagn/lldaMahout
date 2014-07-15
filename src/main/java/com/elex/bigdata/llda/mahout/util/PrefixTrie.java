package com.elex.bigdata.llda.mahout.util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/11/14
 * Time: 3:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class PrefixTrie {
  private Node root;

  public PrefixTrie(){
    root = new Node();
  }

  public void insert(String word,int category){
    if(search(word) !=-1 ) return;

    Node current = root;
    for(int i = 0; i < word.length(); i++){
      Node child = current.subNode(word.charAt(i));
      if(child != null){
        current = child;
      } else {
        current.childMap.put(word.charAt(i), new Node());
        current = current.subNode(word.charAt(i));
      }
      current.count++;
    }
    // Set isEnd to indicate end of the word
    current.category = category;
  }
  public int search(String word){
    Node current = root;

    for(int i = 0; i < word.length(); i++){
      current = current.subNode(word.charAt(i));
      if(current == null)
        return -1;
    }
        /*
        * This means that a string exists, but make sure its
        * a word by checking its 'isEnd' flag
        */
    return current.category;
  }

  public int prefixSearch(String word){
    Node current = root;

    for(int i = 0; i < word.length(); i++){
      current=current.subNode(word.charAt(i));
      if(current == null)
        return -1;
      else if(current.category!=-1)
        return current.category;
    }
    return current.category;
  }

  public void deleteWord(String word){
    if(search(word) == -1) return;

    Node current = root;
    for(char c : word.toCharArray()) {
      Node child = current.subNode(c);
      if(child.count == 1) {
        current.childMap.remove(c);
        return;
      } else {
        child.count--;
        current = child;
      }
    }
    current.category=-1;
  }
  public static class Node{
    int category;
    int count;  // the number of words sharing this character
    Map<Character,Node> childMap;

    public Node(){
      childMap = new HashMap<Character,Node>();
      count = 0;
      category=-1;
    }

    public Node subNode(char c){
      return childMap.get(c);
    }
  }

}
