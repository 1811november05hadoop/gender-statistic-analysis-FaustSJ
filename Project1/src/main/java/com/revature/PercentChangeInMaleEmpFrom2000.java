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
 *  * List the % of change in male employment from the year 2000
 * List the % of change in female employment from the year 2000
 * Do you have a specific country in mind? list of countries?
 *	There is a World estimate if you would want that.
 * 
 * 28716 
 * 28718
 */
public class PercentChangeInMaleEmpFrom2000 extends Configured implements Tool {
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
