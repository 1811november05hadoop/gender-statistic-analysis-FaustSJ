package com.revature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;



public class WordCount {

	public static void main(String[] args) throws Exception{
		
		//code altered and moved to WordCountRunner
		
		//configuration is optional
		//Configuration without parameters just defaults to normal hadoop configurations
		int exitCode = ToolRunner.run(new Configuration(), new WordCountRunner(), args);
		
		System.exit(exitCode);
		
		
//		//ducking all exceptions. Let the JVM catch them in a dead thread
//		
//		if(args.length != 2) {
//			System.err.println("Usage: Wordcount <input dir> <output dir>");
//			System.exit(-1);
//		}
//		//The MapReduce job object
//		Job job = new Job();
//		//The class that contains the main method
//		job.setJarByClass(WordCount.class);
//		
//		job.setJobName("Welcome to the MapReduce");
//		
//		//setting input and output paths
//		//(job, location)
//		FileInputFormat.setInputPaths(job, new Path(args[0]));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
//		
//		//Specify the custom mapper and reducer classes
//		//will accept any classes that extend Mapper or Reducer
//		job.setMapperClass(WordMapper.class);
//		
//		//partitioner class
//		job.setPartitionerClass(AlphabetPartitioner.class);
//		//this by itself doesn't work because a partitoner implies 
//		//  	 "number of reducer tasks", so we need to specify the 
//		//		number of reducers:
//		job.setNumReduceTasks(3);
//		//you will get 1 output PER REDUCER
//		job.setCombinerClass(SumReducer.class);
//		job.setReducerClass(MaxReducer.class);
//		
//		//Specify job final output key value types
//		//Text, IntWritable
//		job.setOutputKeyClass(Text.class);
//		job.setOutputValueClass(IntWritable.class);
//		
//		//Run and check success
//		//wait for end because it's Batch Processing
//		//		rather than do isCompelte()
//		boolean success = job.waitForCompletion(true);
//		System.exit(success ? 0 : 1);
//		
//		//0 = ended well
//		//1 = something else happened
//		//-1 = error
//		//Hadoop will read and interpret these numbers
//		
//		//IN ORDER TO RUN THIS IN ECLIPSE you would need the data here.
	}
}
