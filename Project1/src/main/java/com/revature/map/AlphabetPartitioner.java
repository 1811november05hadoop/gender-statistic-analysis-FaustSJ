package com.revature.map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

//key, value
//the intermediate output of the mapper
public class AlphabetPartitioner extends Partitioner<Text, IntWritable>{
	//first characters like modulars
	private static char FIRST_SECTION_END = 'g';
	
	private static char SECOND_SECTION_START = 'h';
	
	private static char SECOND_SECTION_END = 'p';
	
	//numReduceTasks = the number of reduce tasks
	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		//we evaluate keys have at least 1 character in WordMapper
		char firstCharacter = key.toString().charAt(0);
		
		if(firstCharacter <= FIRST_SECTION_END){
			//send it to the first reducer
			return 0;
		} else if (firstCharacter <= SECOND_SECTION_START && 
				firstCharacter <= SECOND_SECTION_END) {
			return 1;
		} else {
			return 2;
		}
	}

}
