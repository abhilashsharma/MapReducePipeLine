package in.dreamlab.iisc.se256;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class JsonMapper extends Mapper<Object , Text, Text, Text> {
	
	public static final Log LOG = LogFactory.getLog(JsonMapper.class);
	long tripleCount=0;
	long literalCount=0;//not unique
	long uniqueLiteralCount=0;
	 protected void setup(Context context) throws IOException, InterruptedException {
        //Configuration conf = context.getConfiguration();

       
    }
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//tripleCount++;
		String line=value.toString();
		String[] strs = line.trim().split("\\t");
		if(strs.length < 3)
			return;
		//context.write(new Text(strs[0] + "," + strs[1]),NullWritable.get());
		//context.write(new Text(strs[2]),NullWritable.get());
	//	if(strs[2].contains("common.document"))
              // 		context.write(new Text("found"),NullWritable.get());
		//System.out.println("Verb:" + strs[1]);
		if(!strs[2].contains("\"")){
			
			context.write(new Text(strs[0]),new Text(strs[1]));
		//	System.out.println("Literal:"+strs[2]);
		}
		
		//SNAP to Undirected	
		/*String[] strs = line.trim().split("\\s+");
		if(strs.length==2){
			context.write(new Text(strs[0]+" " +strs[1]),new IntWritable(1));
			context.write(new Text(strs[1]+" " +strs[0]),new IntWritable(1));
		}*/


		//OLDER
		/*if(strs.length ==3){
		 	context.write(new Text(strs[0]),new IntWritable(1));
			context.write(new Text(strs[2]),new IntWritable(1));
		}
		else if(strs.length>3) {
			context.write(new Text(strs[1]),new IntWritable(1));
			context.write(new Text(strs[3]),new IntWritable(1));
		
		}*/

		/*for(int i=0;i<strs.length;i++){
			
			System.out.println("TESTM: word is "+strs[i]);
			
			context.write(new Text(strs[i]), new IntWritable(1));
			
		}*/
		

	}

 protected void cleanup(Context context) throws IOException, InterruptedException {
        //Configuration conf = context.getConfiguration();
	//context.write(new Text("tripleCount"),new Text(String.valueOf(tripleCount)));
	//context.write(new Text("literalCount"),new Text(String.valueOf(literalCount)));
  
    }
}
