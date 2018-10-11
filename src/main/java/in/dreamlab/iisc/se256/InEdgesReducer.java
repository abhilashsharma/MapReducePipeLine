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

public class InEdgesReducer extends Reducer<Text,Text,Text, NullWritable> {
	long vertexCount=0;
	long edgeCount=0;
	long literalCount=0;
	long uniqEdges=0;

	
	@Override
	protected void reduce(Text key, Iterable<Text> values , Context context)
			throws IOException, InterruptedException {
	List<String> localList=new ArrayList<String>();	
	List<String> remoteList=new ArrayList<String>();
	for(Text v: values) {
	  String val= v.toString();
	  if(val.contains("l")) {
		  localList.add(val.substring(0, val.length() - 1));
	  }else {
		  remoteList.add(val.substring(0, val.length() - 1));
	  }
	}
	
	String adjList="";
	long count=0;
	for(String lv:localList) {
		if(!(lv.equals(""))) {
		if(count==0) {
			adjList=lv;
		}
		else
		{
			adjList=adjList + ":" + lv;
		}
		count++;
		}

	}
	
	for(String rv:remoteList) {
		if(count==0) {
			adjList=rv;
		}
		else
		{
			adjList=adjList + ":" + rv;
		}

	}
	String output="#"+key.toString()+"@"+count+"%"+adjList+"|";
	context.write(new Text(output), NullWritable.get());
	
	}

 protected void cleanup(Context context) throws IOException, InterruptedException {
	 
	 
	}
}
