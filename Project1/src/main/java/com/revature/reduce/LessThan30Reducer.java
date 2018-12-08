package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LessThan30Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	// TODO Auto-generated method stub
	
	//reduce(input key, input values list, output)
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {

		for(DoubleWritable value: values){
			if(value.get() <= 30.0) {
				context.write(key, value);
			}
		}
	}
}
