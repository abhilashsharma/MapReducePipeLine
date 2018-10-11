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

public class GodbxToMetisReducer extends Reducer<Text,Text,Text, NullWritable> {
	long vertexCount=0;
	long edgeCount=0;
	long literalCount=0;
	long uniqEdges=0;

	
	@Override
	protected void reduce(Text key, Iterable<Text> values , Context context)
			throws IOException, InterruptedException {
	
		String adjList= values.iterator().next().toString();
		
		context.write(new Text(adjList), NullWritable.get());
		
	}

 protected void cleanup(Context context) throws IOException, InterruptedException {
	 
	 
	}
}
