package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//THIS IS HOW YOU FILTER. USE STATIC VARIABLES.

//Returns the absolute max value and its key in the entire dataset

//this is the "final reducer". the other reducer is the "intermediate reducer"
//output of intermediate reducer as this reducer's input
public class MaxReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	//you coouuld have max start as the first value, instead
	public static volatile int CURRENT_MAX_COUNT = Integer.MIN_VALUE;
	//volatile = atomic = either complete the whole process or don't do it at all
	//not needed, but for multi-threading
	public static volatile String CURRENT_MAX_WORD = null;
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		throws IOException, InterruptedException {
		/*
		 * int wordCount = 0;
		
		for(IntWritable value: values){
			wordCount += value.get();
		}
		
		//writing final output
		context.write(key, new IntWritable(wordCount));
		 */
		
		boolean itChanged = false;
		for(IntWritable value : values){
			if(value.get() > CURRENT_MAX_COUNT){
				CURRENT_MAX_COUNT = value.get();
				CURRENT_MAX_WORD = key.toString();
				itChanged = true;
			}
			
			//another potential way to write the assignment, though not as efficient:
//			CURRENT_MAX_COUNT = (value.get() > CURRENT_MAX_COUNT) ?
//					value.get() : CURRENT_MAX_COUNT;
//			CURRENT_MAX_WORD = (value.get() == CURRENT_MAX_COUNT) ? 
//					key.toString() : CURRENT_MAX_WORD;
		}
		
		//only write to output if the value changes
		if(itChanged) {
			context.write(new Text(CURRENT_MAX_WORD), new IntWritable(CURRENT_MAX_COUNT));
		}
		
		/*
		 * You can put your logic in the Mapper.cleanup(Context) method (or Mapper.close() 
		 * for the old mapred api), this is called after the last record has been processed by 
		 * your map method.
		 */
		
	}
}
