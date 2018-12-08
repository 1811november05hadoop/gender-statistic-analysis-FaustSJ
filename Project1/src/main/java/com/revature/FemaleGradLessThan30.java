package com.revature;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.revature.map.AlphabetPartitioner;
import com.revature.map.WordMapper;
import com.revature.reduce.MaxReducer;
import com.revature.reduce.SumReducer;

/*
 *  * Identify the countries where % of female graduates is less than 30%
 * Do you have a particular year in mind? I could do 2016, but 2014 has the most
 * 	data available.
 * By "graduates" are you referring to the population in Masters and Doctorate programs?
 * 	Or are you referring to the population that graduates from tertiary schools.
 */

public class FemaleGradLessThan30 extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
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


		job.setMapperClass(WordMapper.class);
		job.setPartitionerClass(AlphabetPartitioner.class);
		job.setNumReduceTasks(3);
		job.setCombinerClass(SumReducer.class);
		job.setReducerClass(MaxReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		boolean success = job.waitForCompletion(true);
		return (success ? 0 : 1);

	}
}
