package org.myorg;

import java.io.*;
import java.util.*;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class fileccount {
  protected static final int REDUCER_TASKS=7;

  public static class MaxCountTuple implements WritableComparable<MaxCountTuple> {
  	private IntWritable count;
  	private Text filename;
  	private Text chara;
  	private IntWritable times;
   	
  	public MaxCountTuple() {
  		this.count = new IntWritable();
  		this.times = new IntWritable();
  		this.filename = new Text();
  		this.chara = new Text();
  	}
  	
  	public MaxCountTuple(Text filename, IntWritable count, Text chara, IntWritable times) {
  		this.count = count;
  		this.times = times;
  		this.filename = filename;
  		this.chara = chara;
  	}
  	
  	public void set(Text filename, IntWritable count, Text chara, IntWritable times) {
  		this.count = count;
  		this.times = times;
  		this.filename = filename;
  		this.chara = chara;
  	}
  	
  	public IntWritable getCount() {
  		return count;
  	}
  	
  	public Text getFilename() {
  		return filename;
  	}
  	
  	public Text getChara() {
  		return chara;
  	}
  	
  	public IntWritable getTimes() {
  		return times;
  	}
  	
  	public void setCount(IntWritable count) {
  		this.count = count;
  	}
  	
  	public void setFilename(Text filename) {
  		this.filename = filename;
  	}
  	
  	public void setChara(Text chara) {
  		this.chara = chara;
  	}
  	
  	public void setTimes(IntWritable times) {
  		this.times = times;
  	}
  	
  	@Override
  	public void readFields(DataInput in) throws IOException {
  		count.readFields(in);
  		times.readFields(in);
  		filename.readFields(in);
  		chara.readFields(in);
  	}
  	
  	@Override
  	public void write(DataOutput out) throws IOException {
  		count.write(out);
  		times.write(out);
  		filename.write(out);
  		chara.write(out);
  	}
  	
  	public String toString() {
  		return filename + "/" + times + "\t" + count;
  	}
  	
  	@Override
  	public int compareTo(MaxCountTuple o) {
  		if (count.compareTo(o.getCount()) > 0) {
  			return 1;
  		} else {
  			return 0;
  		}
  	}
  	
  	@Override
  	public boolean equals(Object o) {
  		if(o instanceof MaxCountTuple) {
  			MaxCountTuple other = (MaxCountTuple) o;
  			return filename.equals(other.filename) && chara.equals(other.chara);
  		}
  		return false;
  	}
  	
  	@Override
  	public int hashCode() {
  		return chara.hashCode();
  	}
    }


  public static class Map extends Mapper<Object, Text, Text, MaxCountTuple> {
    private final static MaxCountTuple outTuple = new MaxCountTuple();
    
    private IntWritable oneCount = new IntWritable();
    private Text letter = new Text();
    private Text filename = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
      
      HashMap<String, Integer> hp = new HashMap<String, Integer>();
      
      for(char ch='a'; ch <='z'; ch++) {
      	hp.put(String.valueOf(ch), 0);
      }
      
      for(char ch='A'; ch <='Z'; ch++) {
      	hp.put(String.valueOf(ch), 0);
      }
      
      while (tokenizer.hasMoreTokens()) {
      	String token = String.valueOf(tokenizer.nextToken().charAt(0));
      	if (hp.get(token) != null) {
      		int c = hp.get(token);
      		hp.remove(token);
      		hp.put(token, c+1);
      	} else {
      		hp.put(token, 1);
      	}
      }
      
      for(java.util.Map.Entry<String, Integer> m : hp.entrySet()) {
        if (m.getValue().compareTo(0) == 0) filename.set(new Text("-"));
        else filename.set(new Text(fileName));
        
        letter.set(m.getKey());
        oneCount.set(m.getValue());
        
        outTuple.set(filename, oneCount, letter, oneCount);
        
        context.write(letter, outTuple);
      }
    }
  }
  


  public static class Reduce extends Reducer<Text, MaxCountTuple, Text, MaxCountTuple> {
    // Our output value Writable
    private final static MaxCountTuple result = new MaxCountTuple();
    
    public void reduce(Text key, Iterable<MaxCountTuple> values, Context context) throws IOException, InterruptedException {
	//Initialize our result
	result.setCount(new IntWritable());
	result.setFilename(new Text());
	result.setTimes(new IntWritable());
	
	int sum = 0;
	int timesMax = 0;
	int resultMax = 0;
	String maxFilename = "-";
	// Iterator through all input values for this key
	for (MaxCountTuple val : values) {
		// If the result's max is less that the value's max
		// Set the result's max to value's
		if ((resultMax == 0) || (val.getCount().get() > resultMax)) {
			resultMax = val.getCount().get();
			maxFilename = val.getFilename().toString();
			timesMax = val.getCount().get();
		}
		sum = sum + val.getCount().get();
	}
	
	// Set out count to the number of input values
	result.getCount().set(sum);
	result.getFilename().set(maxFilename);
	result.getTimes().set(timesMax);
	context.write(key, result);
   }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf);
    job.setJarByClass(fileccount.class);
    job.setJobName("fileccount");

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(MaxCountTuple.class);

    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    
    //job.setMapOutputKeyClass(Text.class);
    //job.setMapOutputValueClass(MaxCountTuple.class);

    job.setNumReduceTasks(REDUCER_TASKS);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
  }
}

