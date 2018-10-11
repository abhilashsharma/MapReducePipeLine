package in.dreamlab.iisc.se256;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TestMapper extends Mapper<Object , Text, Text, Text> {
	
	public static final Log LOG = LogFactory.getLog(TestMapper.class);
	long tripleCount=0;
	long literalCount=0;//not unique
	long uniqueLiteralCount=0;
	 protected void setup(Context context) throws IOException, InterruptedException {
        //Configuration conf = context.getConfiguration();

       
    }
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		 Path filePath = ((FileSplit) context.getInputSplit()).getPath();
		 String filePathString = ((FileSplit) context.getInputSplit()).getPath().toString();
		 String[] data=filePathString.split(Pattern.quote("/"));
		 String fileName=data[data.length-1];
		 Text outKey,outValue;
		 String line=value.toString();
		 try {
		 if(fileName.contains("Outedges")) {
			 line=line.trim().substring(1, line.length());
			 String[] outData=line.trim().split(Pattern.quote("@"));
			 if(outData.length>1) {
			 String vid=outData[0];
			 context.write(new Text(vid), new Text(outData[1] + "OUT"));
			 }
		 }
		 else if(fileName.contains("Inedges")) {
			 line=line.trim().substring(1, line.length());
			 String[] inData=line.trim().split(Pattern.quote("@"));
			 if(inData.length>1) {
			 String vid=inData[0];
			 context.write(new Text(vid), new Text(inData[1]+ "IN") );
			 }
		 }else {//else it is vertex property
			 line=line.trim().substring(1, line.length());
			 String[] vertexData=line.trim().split(Pattern.quote("@"));
			 if(vertexData.length > 1) {
			 String vid=vertexData[0];
			 context.write(new Text(vid), new Text(vertexData[1] + "VER"));
			 }
		 }
		 }catch(Exception e) {
			 e.printStackTrace();
			 throw new IOException("Exception in line:" + line + " Message:" + e.getMessage() + " for fileName:" + fileName);
		 }

		 
	}

 protected void cleanup(Context context) throws IOException, InterruptedException {

	 
	
    }
}
