package in.dreamlab.iisc.se256;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class HeuristicsReducer extends Reducer<Text,Text,Text, Text> {
	long vertexCount=0;
	long edgeCount=0;
	long literalCount=0;
	long uniqEdges=0;

	protected void setup(Context context) throws IOException, InterruptedException {
		
		 
	       
    }
	
	@Override
	protected void reduce(Text key, Iterable<Text> values , Context context)
			throws IOException, InterruptedException {
	
		long localOutEdgeCount=0;
		long remoteOutEdgeCount=0;
		
		long localInEdgeCount=0;
		long remoteInEdgeCount=0;
		
		long count=0;
		for(Text v:values) {
			String[] val= v.toString().split(",");
			localOutEdgeCount += Long.valueOf(val[0]);
			remoteOutEdgeCount += Long.valueOf(val[1]);
			
			localInEdgeCount += Long.valueOf(val[2]);
			remoteInEdgeCount += Long.valueOf(val[3]);
			count++;
		}
		
		String v=count +"," + localOutEdgeCount + "," + remoteOutEdgeCount+ "," + localInEdgeCount + "," + remoteInEdgeCount; 
		context.write(key, new Text(v));
	}

 protected void cleanup(Context context) throws IOException, InterruptedException {
	 
	 
	}
}
