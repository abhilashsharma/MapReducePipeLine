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

public class WordMapper extends Mapper<Object , Text, Text, Text> {
	
	public static final Log LOG = LogFactory.getLog(WordMapper.class);
	long tripleCount=0;
	long literalCount=0;//not unique
	long uniqueLiteralCount=0;
	 protected void setup(Context context) throws IOException, InterruptedException {
        //Configuration conf = context.getConfiguration();

       
    }
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String str=value.toString();
		String[] data = str.split("\\s+");
		String srcid=data[0];
		String blockid=data[1];
		String srcPid=data[2];
		ArrayList<String> localSinkList= new ArrayList<String>();
		ArrayList<String> remoteSinkList= new ArrayList<String>();
		for(int i=3;i<data.length;i=i+3) {
			if(data[i+2].equals(srcPid)) {
				localSinkList.add(data[i]);
			}
			else {
				remoteSinkList.add(data[i]);
			}
		}
		
		long localEdgeCount=localSinkList.size();
		
		StringBuilder vertexAdj= new StringBuilder("");
		vertexAdj.append(blockid).append("#").append(srcid).append("@").append(localSinkList.size()).append("%");
		if(localSinkList.size()>0) {
			
			for(String localSink:localSinkList) {
				vertexAdj.append(localSink).append(":");
			}
			
		}
		
		if(remoteSinkList.size()>0) {
			for(String remoteSink:remoteSinkList) {
				vertexAdj.append(remoteSink).append(":");
			}
		}
		
		
		if(remoteSinkList.size()>0 || localSinkList.size()>0) {
			vertexAdj.setCharAt(vertexAdj.length()-1, '|');
			
		}else {
			vertexAdj.append("|");
		}

		
		
		context.write(new Text(srcPid), new Text(vertexAdj.toString()));
	}

 protected void cleanup(Context context) throws IOException, InterruptedException {
        //Configuration conf = context.getConfiguration();
	//context.write(new Text("tripleCount"),new Text(String.valueOf(tripleCount)));
	//context.write(new Text("literalCount"),new Text(String.valueOf(literalCount)));
  
    }
}
