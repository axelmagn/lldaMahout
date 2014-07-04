package com.elex.bigdata.llda.mahout.mapreduce.analysis;

import com.elex.bigdata.llda.mahout.data.inputformat.CombineTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 7/3/14
 * Time: 5:55 PM
 * To change this template use File | Settings | File Templates.
 */
public class WordAnalysisDriver {
  public static final String SPECIAL = "www.special.jpeg";
  public static final String REPEAT = "repeat";
  public static final String NOREPEAT = "noRepeat";

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
    Path inputPath = new Path(args[0]);
    Path outputPath = new Path(args[1]);
    Configuration conf = new Configuration();
    conf.setLong("mapred.max.split.size", 5 * 1024 * 1024 * 1024); // 5G
    conf.setLong("mapreduce.input.fileinputformat.split.maxsize", 5 * 1000 * 1000 * 1000);

    Job job = new Job(conf);
    FileSystem fs = FileSystem.get(conf);
    if (fs.exists(outputPath))
      fs.delete(outputPath);
    job.setMapperClass(WordAnalysisMapper.class);
    job.setReducerClass(WordAnalysisReducer.class);
    job.setCombinerClass(WordAnalysisCombiner.class);
    //job.setReducerClass(WordAnalysisCombiner.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setInputFormatClass(CombineTextInputFormat.class);
    FileInputFormat.addInputPath(job, inputPath);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setJarByClass(WordAnalysisDriver.class);
    job.setJobName("word analysis " + inputPath.toString());
    job.submit();
    job.waitForCompletion(true);
  }

  public static class WordAnalysisMapper extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] tokens = line.split("\t");
      if (tokens.length < 3)
        return;
      context.write(new Text(tokens[1]), new IntWritable(Integer.parseInt(tokens[2])));
    }
  }

  public static class WordAnalysisCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    Pattern pattern = Pattern.compile("(gif|GIF|jpg|JPG|png|PNG|ico|ICO|css|CSS|sit|SIT|eps|EPS|wmf|WMF|zip|ZIP|ppt|PPT|mpg|MPG|xls|XLS|gz|GZ|" +
      "pm|RPM|tgz|TGZ|mov|MOV|exe|EXE|jpeg|JPEG|bmp|BMP|js|JS)$");
    private int[] wordLens = new int[]{10, 30, 50, 100, 150, 200, 250, 300, 350, 400, 500, 600, 700, 800, 900, 1000};
    private String[] replaceStr = new String[wordLens.length + 1];
    private int[] wordCounts = new int[wordLens.length + 1];
    private int specialUrlCount = 0;
    private int keyNum = 0;

    public void setup(Context context) {
      Arrays.fill(wordCounts, 0);
      for (int i = 0; i < replaceStr.length - 1; i++) {
        char[] str = new char[wordLens[i]];
        Arrays.fill(str, 'a');
        replaceStr[i] = new String(str);
        System.out.println(replaceStr[i].length());
      }
      char[] str = new char[wordLens[wordLens.length - 1] + 100];
      Arrays.fill(str, 'a');
      replaceStr[replaceStr.length - 1] = new String(str);
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) {
      keyNum += 1;
      int count = 0;
      Iterator<IntWritable> iter = values.iterator();
      while (iter.hasNext())
        count += iter.next().get();
      if (!key.toString().equals(SPECIAL)) {
        int wordLen = key.getLength();
        int i;
        for (i = 0; i < wordLens.length; i++) {
          if (wordLen <= wordLens[i]) {
            break;
          }
        }
        wordCounts[i] += count;
      }
      Matcher matcher = pattern.matcher(key.toString());
      if (matcher.find())
        specialUrlCount += count;
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
      System.out.println("keyNum " + keyNum);
      for (int i = 0; i < wordCounts.length; i++) {
        context.write(new Text(replaceStr[i]), new IntWritable(wordCounts[i]));
        System.out.println(REPEAT + " " + i + " " + wordCounts[i]);
      }
      context.write(new Text(SPECIAL), new IntWritable(specialUrlCount));
    }
  }

  public static class WordAnalysisReducer extends Reducer<Text, IntWritable, Text, Text> {
    private int[] wordLens = new int[]{10, 30, 50, 100, 150, 200, 250, 300, 350, 400, 500, 600, 700, 800, 900, 1000};
    private int[] wordCounts = new int[wordLens.length + 1];
    private int[] noRepeatWordCounts = new int[wordLens.length + 1];

    private int specialUrlCount = 0, totalWordCount = 0, noRepeatTotalWordCount = 0;

    public void setup(Context context) {
      Arrays.fill(wordCounts, 0);
      Arrays.fill(noRepeatWordCounts, 0);
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context) {
      String[] tokens = key.toString().split(" ");
      if (tokens.length < 2)
        return;
      int index = Integer.parseInt(tokens[1]);
      if (index < 0 || index > wordCounts.length - 1) {
        System.out.println("index is " + index + ". error!");
        return;
      }

      int count = 0;
      Iterator<IntWritable> iter = values.iterator();
      while (iter.hasNext())
        count += iter.next().get();
      if (tokens[0].equals(REPEAT)) {
        wordCounts[index] += count;
        totalWordCount += count;
      } else if (tokens[0].equals(NOREPEAT)) {
        noRepeatWordCounts[index] += count;
        noRepeatTotalWordCount += count;
      } else if (tokens[0].equals(SPECIAL)) {
        specialUrlCount += count;
      }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
      context.write(new Text("total count"), new Text(" " + totalWordCount));

      context.write(new Text("repeat word count"), new Text(wordCounts.length + " groups"));
      context.write(new Text("~" + wordLens[0]), new Text(" " + wordCounts[0]));
      for (int i = 1; i < wordCounts.length - 1; i++) {
        context.write(new Text(wordLens[i - 1] + "~" + wordLens[i]), new Text(" " + wordCounts[i]));
      }
      context.write(new Text(wordLens[wordLens.length - 1] + "~"), new Text(" " + wordCounts[wordCounts.length - 1]));
      context.write(new Text("no repeat total count"), new Text(" " + noRepeatTotalWordCount));
      context.write(new Text("no repeat word count"), new Text(noRepeatWordCounts.length + " groups"));
      context.write(new Text("~" + wordLens[0]), new Text(" " + noRepeatWordCounts[0]));
      for (int i = 1; i < noRepeatWordCounts.length - 1; i++) {
        context.write(new Text(wordLens[i - 1] + "~" + wordLens[i]), new Text(" " + noRepeatWordCounts[i]));
      }
      context.write(new Text(wordLens[wordLens.length - 1] + "~"), new Text(" " + noRepeatWordCounts[noRepeatWordCounts.length - 1]));
      context.write(new Text("special word "), new Text(" " + specialUrlCount));
    }


  }

}
