package com.elex.bigdata.llda.mahout.data;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.junit.Test;

import java.io.DataInput;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 6/13/14
 * Time: 4:32 PM
 * To change this template use File | Settings | File Templates.
 */
public class FileSystemTest {
  @Test
  public void testFileStatus() throws IOException, URISyntaxException {
    Path path=new Path("/home/yb/windows/share/categoryFilter");
    FileSystem fs=new RawLocalFileSystem();
    fs.initialize(new URI("localFs"),new Configuration());
    fs.exists(path);
    FileStatus fileStatus=fs.getFileStatus(path);
    FileStatus[] fileStatuses= fs.listStatus(path);
    for(FileStatus fileStatus1:fileStatuses){
      //System.out.println(fileStatus1.getPath().getName());
      //System.out.println(fileStatus1.getPath().toString());
    }
    DataInput dataInput=fs.open(new Path("/home/yb/windows/share/categoryFilter/Top.Arts.Animation"));
    BloomFilter bloomFilter=new BloomFilter();
    bloomFilter.readFields(dataInput);
    System.out.println("hhh") ;
  }
  @Test
  public void testFile(){
    File file=new File("/home/yb/windows");
    System.out.println(file.toString());
    System.out.println(file.getName());
    File childFile=new File(file,"share");
    System.out.println(childFile.toString());
  }
  @Test
  public void testHex() throws DecoderException {
    String str="0a";
    byte[] bytes=Hex.decodeHex(str.toCharArray());
    System.out.println(Bytes.toShort(bytes));
  }
  @Test
  public void testRegex(){
    Pattern pattern=Pattern.compile("(gif|GIF|jpg|JPG|png|PNG|ico|ICO|css|CSS|sit|SIT|eps|EPS|wmf|WMF|zip|ZIP|ppt|PPT|mpg|MPG|xls|XLS|gz|GZ|" +
      "pm|RPM|tgz|TGZ|mov|MOV|exe|EXE|jpeg|JPEG|bmp|BMP|js|JS)$");
    Matcher matcher=pattern.matcher("www.google.com/bmp.hello");
    System.out.println(matcher.find());
  }
}
