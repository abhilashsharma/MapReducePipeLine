package in.dreamlab.iisc.se256;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SnapToMetisRangePartitioner extends Partitioner<Text,Text>{
	@Override
	public int getPartition(Text key, Text value, int numReduceTasks){
	if(numReduceTasks==0)
		return 0;
	long src=Long.valueOf(key.toString());
	if(src < 3500000l) {
		return 0;
	}
	else if(src < 7000000l) {
		return 1;
	}
	else if(src< 10500000l) {
		return 2;
	}
	else if(src<14000000l) {
		return 3;
	}else if(src<17500000l) {
		return 4;
	}else if(src<21000000l) {
		return 5;
	}else if(src<25000000l) {
		return 6;
	}else if(src<30000000l) {
		return 7;
	}
	return 0;//default case
	}

	
}
