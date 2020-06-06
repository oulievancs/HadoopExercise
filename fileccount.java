package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class fileccount {
  protected static final int REDUCER_TASKS=3;

  public class MaxCountTuple implements Writable {
  	private int max = 0;
  	private long count = 0;
  	private String filename = "";
  	
  	public int getMax() {
  		return max;
  	}
  	
  	public long getCount() {
  		return count;
  	}
  	
  	public String getFilename() {
  		return filename;
  	}
  	
  	public void setMax(int max) {
  		this.max = max;
  	}
  	
  	public void setCount(int count) {
  		this.count = count;
  	}
  	
  	public void setFilename(String filename) {
  		this.filename = filename;
  	}
  	
  	public void readFields(DataInput in) throws IOException {
  		max = in.readInt();
  		count = in.readLong();
  		filename = in.readString();
  	}
  	
  	public void write(DataOutput out) throws IOException {
  		out.writeInt(max);
  		out.writeLong(count);
  		out.writeString(filename);
  	}
  	
  	public String toString() {
  		return max + "\t" + filename + "\t" + count;
  	}

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text c = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      String filename = ((FileSplit) reporter.getInputSplit()).getPath().getName();
      String line = value.toString();

      String words[] = line.split("");
      String firsts = "";
      int i=0;
      for (String s : words) {
	firsts += s.charAt(0) + " ";
      }
      StringTokenizer tokenizer = new StringTokenizer(firsts);
      
      while (tokenizer.hasMoreTokens()) {
        c.set(tokenizer.nextToken()+","+filename);
        output.collect(c, one);
      }
    }
  }

  public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private Text c = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      String filename = ((FileSplit) reporter.getInputSplit()).getPath().getName();
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line, "\n", false);
      
      
      while (tokenizer.hasMoreTokens()) {
      	StringTokenizer tokenizer_tmp = new StringTokenizer(tokenizer.nextToken(), ",", false);
      	
        c.set(tokenizer_tmp.nextToken());
        
        StringTokenizer tokenizer_tmp1 = new StringTokenizer(tokenizer_tmp.nextToken(), "\t", false);
        tokenizer_tmp1.nextToken();
        
        IntWritable one = new IntWritable(Integer.parseInt(tokenizer_tmp1.nextToken()));
        output.collect(c, one);
      }
      
      tokenizer = new StringTokenizer(line, "\n", false);
      while (tokenizer.hasMoreTokens()) {
      	StringTokenizer tokenizer_tmp1 = new StringTokenizer(tokenizer.nextToken(), "\t", false);
      	c.set(tokenizer_tmp1.nextToken());
      	
      	IntWritable one = new IntWritable(Integer.parseInt(tokenizer_tmp1.nextToken()));
      	output.collect(c, one);
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	int sum = 0;
      	while (values.hasNext()) {
        	sum += values.next().get();
      	}
        
      	output.collect(key, new IntWritable(sum));
   }
  }

  public static class Reduce1 extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	if (key.contains('
		int sum = 0;
	      	while (values.hasNext()) {
			sum += values.next().get();
	      	}
		
	      	output.collect(key, new IntWritable(sum));
   }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(fileccount.class);
    conf.setJobName("fileccount");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setNumReduceTasks(REDUCER_TASKS);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);

    /* Start the 2nd MR */
    conf = new JobConf(fileccount.class);
    conf.setJobName("fileccountfinal");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(Map1.class);
    conf.setCombinerClass(Reduce1.class);
    conf.setReducerClass(Reduce1.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    conf.setNumReduceTasks(REDUCER_TASKS);

    FileInputFormat.setInputPaths(conf, new Path(args[1]));
    FileOutputFormat.setOutputPath(conf, new Path(args[2]));

    JobClient.runJob(conf);
  }
}

