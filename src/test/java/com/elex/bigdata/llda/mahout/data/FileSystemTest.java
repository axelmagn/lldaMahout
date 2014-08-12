package com.elex.bigdata.llda.mahout.data;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
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
    Pattern anoPattern= Pattern.compile("\\.(gif|GIF|jpg|JPG|png|PNG|ico|ICO|css|CSS|sit|SIT|eps|EPS|wmf|WMF|zip|ZIP|ppt|PPT|mpg|MPG|xls|XLS|gz|GZ|" +
      "pm|RPM|tgz|TGZ|mov|MOV|exe|EXE|jpeg|JPEG|bmp|BMP|js|JS)(\\?.+)?$");
    Matcher matcher=anoPattern.matcher("www.google.com/hello.bmp?");
    System.out.println(matcher.find());
    String str="showads.pubmatic.com/adserver/adserverservlet?pubid=50055&siteid=50058&adid=90495&kadwidth=728&kadheight=90&saversion=2&js=1&kdntuid=1&pageurl=http%3a%2f%2fib.adnxs.com%2ftt%3fid%3d2785280%26referrer%3d%24%7breferer_url%7d&refurl=http%3a%2f%2fib.adnxs.com%2ftt%3fid%3d3124862%26referrer%3dhttp%253a%252f%252fwww.365vivo.com%252flogin.aspx%26cb%3d327593506&iniframe=1&kadpageurl=%24%7breferer_url%7d&operid=3&kltstamp=2014-7-2%2017%3a57%3a42&timezone=2&screenresolution=1366x768&ranreq=0.16519579873420298&pmuniadid=0&at_payload=k8a44ifbbnly5du4uxukrnz2ci9xkprwvl6tqahbrmqklrus_43wuzpup_nh2t05oayahrcpmxe6dbur5xj6kkt8fsbq91nbbqgxk_pmtd0shp815lyjay2.rinj.rin4wzcjftckckyad65hz74wysxvoxwaw4b6y8ggedd5ihorovyfgh8cmvsuckzilny6xljqlprdhproxxvgn8xf7_olgipfmsnrmpsb6meqhvaupfnmbsu1nbjlpmpwonsuc56mngwpwonn5uq32scvcnfd9..4hevmatjs0m_djjky_aw7q_h.4tfsqr95qdxmejv.lv9dwgodmx.ua9zc9z1euvr9z.amvurnw5cfuxtstkje4pidxo9sprsimtkqnllznjwcquhky5bspbkw.4yl";
    System.out.println(str.length());
    Text text=new Text(str);
    System.out.println(text.toString());
    System.out.println(text.getLength());
    Pattern cleanPattern=Pattern.compile("([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+)|(:[0-9]+$)|(^/|/$)|(^[^.]+$)|" +
      "(.*(gravatar|msn|microsoft|twitter|xingcloud|log\\.optimizely)\\.com.*)|(.*bing\\.net.*)|(.*goo\\..*)|" +
      "(.*xxx.*)|(\\.pl$)|(\\.crl$)|(\\.srf$)|(\\.fcgi$)|(\\.cgi$)|(\\.xgi$)");
    String[] testStrs=new String[]{"101.2.23.1","/page","hello.com:443","hello.com//","adbdcdd","gravatar.com.cn/gravatar",
                                   "ar.msn.com","www.bing.net","ar.goo.mx/pl","sunxxx.com","inet.com/inet.pl","www.amazon.com"};
    for(int i=0;i<testStrs.length;i++){
      System.out.println(testStrs[i]+"\t"+cleanPattern.matcher(testStrs[i]).find());
    }

    Pattern pattern1=Pattern.compile(".*(gravatar|msn|microsoft|twitter|xingcloud|log\\.optimizely)\\.com.*");
    for(int i=0;i<testStrs.length;i++){
      System.out.println(testStrs[i]+"\t"+pattern1.matcher(testStrs[i]).find());
    }
    Pattern pattern2=Pattern.compile("^[^.]+$");
    System.out.println(pattern2.matcher("adbdcdd").find());
  }
}
