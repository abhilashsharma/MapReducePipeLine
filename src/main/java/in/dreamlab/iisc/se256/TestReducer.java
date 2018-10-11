package in.dreamlab.iisc.se256;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.mortbay.log.Log;

public class TestReducer extends Reducer<Text,Text,Text, Text> {
	long vertexCount=0;
	long edgeCount=0;
	long literalCount=0;
	long uniqEdges=0;

	
	@Override
	protected void reduce(Text key, Iterable<Text> values , Context context)
			throws IOException, InterruptedException {
		
		String line="";
		String[] data=null;
		try {
		String outData="",inData="",vertexData="";
		for (Text v: values) {
			line=v.toString();
			data=line.split(Pattern.quote("|"));
			if(data[1].equals("OUT")) {
				outData=data[0];
			}else if(data[1].equals("IN")) {
				inData=data[0];
			}else {
				vertexData=data[0];
			}

		}
		
		context.write(key, new Text(outData + "|" +inData + "|" + vertexData));
		
		}catch(Exception e) {
			throw new IOException("Exception at line:" + line);
		}
		
		
	}

 protected void cleanup(Context context) throws IOException, InterruptedException {
	 
	 
	}
}
