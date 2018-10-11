package in.dreamlab.iisc.se256;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class WordCombiner extends Reducer<Text,Text,Text, Text> {
	long vertexCount=0;
	@Override
	protected void reduce(Text key, Iterable<Text> values , Context context)
			throws IOException, InterruptedException {
		
	
		String adjList="";
		for(Text v: values) {
			
			adjList= adjList + " " + v.toString();
		}

		
		context.write(key, new Text(adjList));
	
		
		}

 protected void cleanup(Context context) throws IOException, InterruptedException {
       
	//context.write(new Text("vertexCount"),new Text(String.valueOf(vertexCount)));
	}
}
