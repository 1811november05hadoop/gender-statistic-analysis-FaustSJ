package com.revature.reduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.revature.models.DoubleArrayWritable;

public class DifferenceReducer extends Reducer<Text, DoubleArrayWritable, Text, Text>{
	
	//reduce(input key, input values list, output)
	public void reduce(Text key, Iterable<DoubleArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		
		//The mapper sends an array as a value, which is added to an iterable.
		for(DoubleArrayWritable doubleArray: values){
			DoubleWritable[] vals = doubleArray.get();
			
			double difference = vals[1].get()-vals[0].get();
			
			//format the output to describe the change
			if(difference>0) {
				context.write(key, new Text("employment percentage increased by "+difference));
			}else if(difference<0) {
				difference *= -1.0;
				context.write(key, new Text("employment percentage decreased by "+difference));
			}else {
				context.write(key, new Text("employment percentage hasn't changed from "+vals[0].get()));
			}
		}
	}
}
