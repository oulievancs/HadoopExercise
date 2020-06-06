package org.myorg;

import java.io.IOException;
import java.util.*;
import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.*;
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

  public static class Map1 extends Mapper implements Mapper<Object, Text, Text, MaxCountTuple> {
    private Text letter = new Text();
    private MaxCountTuple outTuple = new MaxCountTuple();

    public void map(Object key, Text value, Context context) throws IOException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line, "\n", false);
      
      
      while (tokenizer.hasMoreTokens()) {
      	StringTokenizer tokenizer_tmp = new StringTokenizer(tokenizer.nextToken(), ",", false);
      	
        String letter_tmp = tokenizer_tmp.nextToken();
        
        StringTokenizer tokenizer_tmp1 = new StringTokenizer(tokenizer_tmp.nextToken(), "\t", false);
        outTuple.setFilename(tokenizer_tmp1.nextToken());
        
        Integer count = Integer.parseInt(tokenizer_tmp1.nextToken());
        outTuple.setMax(count);
        outTuple.setCount(count);
        
        letter.set(letter_tmp);
        context.write(letter, outTuple);
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

  public static class Reduce1 extends Reducer implements Reducer<Text, MaxCountTuple, Text, MaxCountTuple> {
    // Our output value Writable
    private MaxCountTuple result = new MaxCountResult();
    public void reduce(Text key, Iterator<MaxCountTuple> values, Context context) throws IOException {
	//Initialize our result
	result.setMax(null);
	result.setCount(null);
	result.setFilename(null);
	
	int sum = 0;
	
	// Iterator through all input values for this key
	while (values.hasNext()) {
		// If the result's max is less that the value's max
		// Set the result's max to value's
		MaxCountTule val = values.nextToken();
		if (result.getMax() == null || val.getMax().compareTo(result.getMax()) > 0) {
			result.setMax(val.getMax());
		}
		sum += val.getCount();
	}
	
	// Set out count to the number of input values
	result.setCount(sum);
	context.write(key, result);
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
    Job conf1 = new Job();
    conf1.setJarByClass(fileccount.class);
    conf.setJobName("fileccountfinal");

    conf1.setMapperClass(Map1.class);
    conf1.setCombinerClass(Reduce1.class);
    conf1.setReducerClass(Reduce1.class);

    conf1.setOutputKeyClass(Text.class);
    conf1.setOutputValueClass(IntWritable.class);
    conf1.setOutputFormatClass(TextOutputFormat.class);

    conf.setNumReduceTasks(REDUCER_TASKS);

    FileInputFormat.setInputPaths(conf1, new Path(args[1]));
    FileOutputFormat.setOutputPath(conf1, new Path(args[2]));

    conf1.waitForCompletion(true);
  }
}

