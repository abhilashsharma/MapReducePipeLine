package in.dreamlab.iisc.se256;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/*
 * Each line is adjacency list of the form similar to succinct edge file i.e. SGID#vid@localedgecount%edge1:edge2:...|
 */
public class InEdgesMapper extends Mapper<Object , Text, Text, Text> {
	
	public static final Log LOG = LogFactory.getLog(InEdgesMapper.class);
	long tripleCount=0;
	long literalCount=0;//not unique
	long uniqueLiteralCount=0;
	 protected void setup(Context context) throws IOException, InterruptedException {
        
		 
       
    }
	@Override
	protected void map(Object key, Text value, Context context)
	throws IOException, InterruptedException {
		
		String line = value.toString().trim();
		try {
		String[] data= line.split("\\W");
		if(data.length>2) {
		String srcSgid = data[0];//TODO:change this # is added
		Long srcVid = Long.parseLong(data[1]);
		int localEdges = Integer.parseInt(data[2]);
	
		context.write(new Text(srcVid.toString()), new Text("l"));
		
		for(int i=3;i<3+localEdges;i++) {
			Long sinkVid= Long.parseLong(data[i]);
//			long sinkBlockid=Long.parseLong(data[i+1]);
//			long sinkSlaveid=Long.parseLong(data[i+2]);
			context.write(new Text(sinkVid.toString()), new Text(srcVid.toString() + "l"));
		}//for ends
		
		for(int i=3+localEdges;i<data.length-1;i++) {
			Long sinkVid= Long.parseLong(data[i]);
//			long sinkBlockid=Long.parseLong(data[i+1]);
//			long sinkSlaveid=Long.parseLong(data[i+2]);
			context.write(new Text(sinkVid.toString()), new Text(srcVid.toString() + "r"));
		}//for ends
		}//if ends
	}
		catch(Exception e) {
			throw new IOException(e.getMessage()+" ----Exception in line:" + line);
		}
			}

 protected void cleanup(Context context) throws IOException, InterruptedException {
        //Configuration conf = context.getConfiguration();
	//context.write(new Text("tripleCount"),new Text(String.valueOf(tripleCount)));
	//context.write(new Text("literalCount"),new Text(String.valueOf(literalCount)));
  
    }
}
