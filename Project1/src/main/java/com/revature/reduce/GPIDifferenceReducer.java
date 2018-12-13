package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.revature.models.DoubleArrayWritable;

public class GPIDifferenceReducer extends Reducer<Text, DoubleArrayWritable, Text, Text>{
	
	//reduce(input key, input values list, output)
	public void reduce(Text key, Iterable<DoubleArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		
		//The mapper sends an array as a value, which is added to an iterable.
		for(DoubleArrayWritable doubleArray: values){
			DoubleWritable[] vals = doubleArray.get();
			
			double difference = vals[1].get()-vals[0].get();
			context.write(key, new Text("The GPI increased by "+difference+" from originally "+vals[0]));
		}
	}
}
