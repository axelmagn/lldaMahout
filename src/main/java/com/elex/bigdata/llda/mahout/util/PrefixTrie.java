package com.elex.bigdata.llda.mahout.util;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/11/14
 * Time: 3:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class PrefixTrie {
  /*
     tree to save preClassified urls
     if a url is prefix of preClassified url ,it should be considered also Classified
   */
  private Node root;
  private int size=0;
  public PrefixTrie() {
    root = new Node();
    size+=1;
  }

  public void insert(String word, int category) {
    if (search(word) != -1) return;

    Node current = root;
    for (int i = 0; i < word.length(); i++) {
      char letter = word.charAt(i);
      if (letter >= 48 && letter <= 57) {
        Node child = current.subNode((int) letter - 48);
        if (child != null) {
          current = child;
        } else {
          Node nextNode = new Node();
          current.nextNodes[(int) letter - 48] = nextNode;
          current = nextNode;
          size+=1;
        }
        current.count++;
      } else if (letter >= 97 && letter <= 122) {
        Node child = current.subNode((int) letter - 97);
        if (child != null) {
          current = child;
        } else {
          Node nextNode = new Node();
          current.nextNodes[(int) letter - 97] = nextNode;
          current = nextNode;
          size+=1;
        }
        current.count++;
      }
    }
    // Set isEnd to indicate end of the word
    current.category = category;
  }

  public int search(String word) {
    Node current = root;

    for (int i = 0; i < word.length(); i++) {
      char letter = word.charAt(i);
      if (letter >= 48 && letter <= 57) {
        current = current.subNode((int) letter - 48);
        if (current == null)
          return -1;
      } else if (letter >= 97 && letter <= 122) {
        current = current.subNode((int) letter - 97);
        if (current == null)
          return -1;
      }
    }
        /*
        * This means that a string exists, but make sure its
        * a word by checking its 'isEnd' flag
        */
    return current.category;
  }

  public int prefixSearch(String word) {
    Node current = root;
    int category=-1;
    for (int i = 0; i < word.length(); i++) {
      char letter = word.charAt(i);
      if (letter >= 48 && letter <= 57) {
        current = current.subNode((int) letter - 48);
        if (current == null)
          return category;
        else if (current.category != -1)
          category=current.category;
      } else if (letter >= 97 && letter <= 122) {
        current = current.subNode((int) letter - 97);
        if (current == null)
          return category;
        else if (current.category != -1)
          category=current.category;
      }
    }
    return category;
  }

  public void deleteWord(String word) {
    if (search(word) == -1) return;

    Node current = root;
    for (char c : word.toCharArray()) {
      if (c >= 48 && c <= 57) {
        Node child = current.subNode((int) c - 48);
        if (child.count == 1) {
          current.nextNodes[(int) c - 48] = null;
          return;
        } else {
          child.count--;
          current = child;
        }
      } else if (c >= 97 && c <= 122) {
        Node child = current.subNode((int) c - 97);
        if (child.count == 1) {
          current.nextNodes[(int) c - 97] = null;
          return;
        } else {
          child.count--;
          current = child;
        }
      }

    }
    current.category = -1;
  }

  public int getSize(){
    return size;
  }

  public static class Node {
    int category;
    int count;  // the number of words sharing this character
    Node[] nextNodes = new Node[10 + 26];

    public Node() {
      Arrays.fill(nextNodes, null);
      count = 0;
      category = -1;
    }

    public Node subNode(int c) {
      return nextNodes[c];
    }
  }

}
