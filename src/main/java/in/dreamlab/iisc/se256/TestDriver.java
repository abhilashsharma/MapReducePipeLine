package in.dreamlab.iisc.se256;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
public class TestDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		
		Configuration conf = new Configuration();
		Path Input_PATH=new Path(args[0]);
		Path Output_PATH=new Path(args[1]);

		conf.set("mapreduce.textoutputformat.separator", ",");
		Job job1 = Job.getInstance(conf, "Graph Stats creation job");

		job1.setJarByClass(TestMapper.class);
	    job1.setMapperClass(TestMapper.class);

	    job1.setReducerClass(TestReducer.class);
	    
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);

	    job1.setMapOutputKeyClass(Text.class);
	    job1.setMapOutputValueClass(Text.class);
//	    job1.setInputFormatClass(TextInputFormat.class);
//	   job1.setCombinerClass(WordCombiner.class);
  	   job1.setNumReduceTasks(14);
	    FileInputFormat.addInputPath(job1, Input_PATH);
	    FileOutputFormat.setOutputPath(job1, Output_PATH);

		
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
		
	
	
	}
	
}
