package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.revature.models.DoubleArrayWritable;

public class AverageReducer extends Reducer<Text, DoubleArrayWritable, Text, DoubleWritable>{
	
	//reduce(input key, input values list, output)
	public void reduce(Text key, Iterable<DoubleArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		
		//The combiner sends an array as a value, which is added to an iterable.
		for(DoubleArrayWritable doubleArray: values){
			DoubleWritable[] doubleWritables = doubleArray.get();
			
			//calcualte the sum of the values
			double sum = 0;
			for(DoubleWritable diff : doubleWritables) {
				sum+=diff.get();
			}
			
			//calculate and write the average to the output
			context.write(key, new DoubleWritable((double)Math.round((sum / ((double)doubleWritables.length)) * 100000d) / 100000d));
		}
	}
}
