package com.elex.bigdata.llda.mahout.data;

import org.junit.Test;

import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 6/11/14
 * Time: 9:45 AM
 * To change this template use File | Settings | File Templates.
 */
public class PasswdGenerator {
  @Test
  public void genPasswd(){
    Random r1=new Random(10000l),r2=new Random(100001l),r3=new Random(101110101l),r4=new Random(133233120221l);
    StringBuilder builder=new StringBuilder();
    for(int i=0;i<10;i++){
      int a=Math.abs(r1.nextInt());
      builder.append((char)((a%74)+48));
      int b=Math.abs(r2.nextInt());
      builder.append((char)((b%74)+48));
      int c=Math.abs(r3.nextInt());
      builder.append((char)((c%74)+48));
      int d=Math.abs(r4.nextInt());
      builder.append((char)((d%74)+48));
    }

    System.out.println(builder.toString());
    /*
    for(int i=0;i<128;i++){
      System.out.println(i+":"+" "+(char)i);
    }
    */
  }
}
