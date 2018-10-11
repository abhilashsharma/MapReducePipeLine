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

public class InEdgesReducerBlogel extends Reducer<Text,Text,Text, Text> {
	long vertexCount=0;
	long edgeCount=0;
	long literalCount=0;
	long uniqEdges=0;

	
	@Override
	protected void reduce(Text key, Iterable<Text> values , Context context)
			throws IOException, InterruptedException {

		String adjList="";
		long count=0;
		for(Text v:values) {
			if(count==0) {
				adjList=v.toString();
			}else {
				adjList+=" " + v.toString();
			}
			count++;
		}
		
		context.write(key, new Text(adjList));
	
	}

 protected void cleanup(Context context) throws IOException, InterruptedException {
	 
	 
	}
}
