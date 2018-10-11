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
public class SnapToMetisMapper extends Mapper<Object , Text, Text, Text> {
	
	public static final Log LOG = LogFactory.getLog(SnapToMetisMapper.class);
	long tripleCount=0;
	long literalCount=0;//not unique
	long uniqueLiteralCount=0;
	 protected void setup(Context context) throws IOException, InterruptedException {
        
		 
       
    }
	@Override
	protected void map(Object key, Text value, Context context)
	throws IOException, InterruptedException {
		
		String line = value.toString().trim();
		String[] data = line.split("\\s+");
		Long src=Long.valueOf(data[0]);
		Long sink=Long.valueOf(data[1]);
		if(src==0l) {
			src=28943748l;
		}
		if(sink==0l) {
			sink=28943748l;
		}
		context.write(new Text(src.toString()), new Text(sink.toString()));
		context.write(new Text(sink.toString()), new Text(src.toString()));
	}

 protected void cleanup(Context context) throws IOException, InterruptedException {
        //Configuration conf = context.getConfiguration();
	//context.write(new Text("tripleCount"),new Text(String.valueOf(tripleCount)));
	//context.write(new Text("literalCount"),new Text(String.valueOf(literalCount)));
  
    }
 
 
 
}
