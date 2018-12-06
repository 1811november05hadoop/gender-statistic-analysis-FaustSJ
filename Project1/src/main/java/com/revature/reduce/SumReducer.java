package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Reducer< intermediate output types (last two types listed in Mapper<>), final output types>
public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	//reduce(input key, input values list, output)
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
		
		int wordCount = 0;
		
		for(IntWritable value: values){
			wordCount += value.get();
		}
		
		//writing final output
		context.write(key, new IntWritable(wordCount));
		
	}
}
