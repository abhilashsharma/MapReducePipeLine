package in.dreamlab.iisc.se256;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;

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
public class HeuristicsMapper extends Mapper<Object , Text, Text, Text> {
	
	public static final Log LOG = LogFactory.getLog(HeuristicsMapper.class);
	long tripleCount=0;
	long literalCount=0;//not unique
	long uniqueLiteralCount=0;
	 protected void setup(Context context) throws IOException, InterruptedException {
        
		 
       
    }
	@Override
	protected void map(Object key, Text value, Context context)
	throws IOException, InterruptedException {
		
		String line = value.toString().trim();
		String[] d=line.trim().split("\\s+");
		String vid=d[0];
		String allData=d[1];
		try {
		String[] data= allData.split(Pattern.quote("|"));
		if(data.length>1) {//valid line check
			String vertexProp=data[2];
			String outEdges=data[0];
			String inEdges=data[1];
			
			String[] prop = vertexProp.split("\\W");//keys are from prop file
			String[] outData = outEdges.split(":");//out edge Files
			String[] inData = inEdges.split(":");//in edge Files
			
			long outEdgeCount = outData.length;
			long localOutEdgeCount = Long.valueOf(outData[0].split("\\W")[0]);
			long remoteOutEdgeCount = outEdgeCount - localOutEdgeCount;

			//TODO: use this when graph is directed
			long inEdgeCount = inData.length;
			long localInEdgeCount = Long.valueOf(inData[0].split("\\W")[0]);
			long remoteInEdgeCount = inEdgeCount - localInEdgeCount;
			
//			long inEdgeCount = outEdgeCount;
//			long localInEdgeCount = localOutEdgeCount;
//			long remoteInEdgeCount = remoteOutEdgeCount;
			
			String v=localOutEdgeCount + "," + remoteOutEdgeCount+ "," + localInEdgeCount + "," + remoteInEdgeCount; 
			
			for(int i=0;i<prop.length;i++) {
				context.write(new Text((i+1)+","+prop[i]), new Text(v));
			}
//			context.write(new Text(0+"," + vid), new Text(v));// 0 used for vid stats, vid is not used anymore
		}//if ends
	}
		catch(Exception e) {
			e.printStackTrace();
			throw new IOException(e.getMessage()+" ----Exception in vid:" + vid + " allData:" + allData);
		}
			}

 protected void cleanup(Context context) throws IOException, InterruptedException {
        //Configuration conf = context.getConfiguration();
	//context.write(new Text("tripleCount"),new Text(String.valueOf(tripleCount)));
	//context.write(new Text("literalCount"),new Text(String.valueOf(literalCount)));
  
    }
}
