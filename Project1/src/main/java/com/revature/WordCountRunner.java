package com.revature;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

public class WordCountRunner extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		//just copy all code from WordCount main()

		if(args.length != 2) {
			System.err.println("Usage: Wordcount <input dir> <output dir>");
			return -1;
		}

		Job job = new Job();
		job.setJarByClass(WordCount.class);

		job.setJobName("Welcome to the MapReduce");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));


//		job.setMapperClass(WordMapper.class);
//		job.setPartitionerClass(AlphabetPartitioner.class);
//		job.setNumReduceTasks(3);
//		job.setCombinerClass(SumReducer.class);
//		job.setReducerClass(MaxReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		boolean success = job.waitForCompletion(true);
		return (success ? 0 : 1);

	}

}
