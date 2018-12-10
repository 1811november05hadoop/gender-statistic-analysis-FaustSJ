package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LessThan30Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	//reduce(input key, input values list, output)
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
		//there should only be one value, but we need to iterate to get it	
		for(DoubleWritable value: values){
			//if the value is less than 30, write it to output.
			if(value.get() < 30.0) {
				context.write(key, value);
				//no need to check for more values or print anymore, just return
				return;
			}
		}
	}
}
