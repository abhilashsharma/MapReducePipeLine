package in.dreamlab.iisc.se256;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/*
 * Each line is adjacency list of the form similar to succinct edge file i.e. SGID#vid@localedgecount%edge1:edge2:...|
 */
public class GodbxToMetisMapper extends Mapper<Object , Text, Text, Text> {
	
	public static final Log LOG = LogFactory.getLog(GodbxToMetisMapper.class);
	long tripleCount=0;
	long literalCount=0;//not unique
	long uniqueLiteralCount=0;
	 protected void setup(Context context) throws IOException, InterruptedException {
        
		 
       
    }
	@Override
	protected void map(Object key, Text value, Context context)
	throws IOException, InterruptedException {
		
		String line = value.toString().trim();
		String[] data = line.split("\\W");
		String src=data[1];
		String adjList=data[3];
		for(int i=3;i<data.length;i++) {
			adjList=adjList+"\t" +data[i];
		}
		context.write(new Text(src.toString()), new Text(adjList));

	}

 protected void cleanup(Context context) throws IOException, InterruptedException {
        //Configuration conf = context.getConfiguration();
	//context.write(new Text("tripleCount"),new Text(String.valueOf(tripleCount)));
	//context.write(new Text("literalCount"),new Text(String.valueOf(literalCount)));
  
    }
 
 
 
}
