package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Mapper< LineNumber, InputType, OutputKeyType, OutputValueType>
//output = intermediate output
public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException{
		//Hadoop throws these exceptions, we duck them
		
		//get a line from the flatfile
		String line = value.toString();
		
		//split the lines through " " spaces
		for(String word: line.split("\\W+")){
			if(word.length() > 0){
				
				//writing intermediate output
				//write(key, value)
				//context.write(word, 1); error!
				//(fix capitalization inconsistencies only if the business doesn't mind)
				context.write(new Text(word.toLowerCase()), new IntWritable(1));
			}
		}
	}
}
