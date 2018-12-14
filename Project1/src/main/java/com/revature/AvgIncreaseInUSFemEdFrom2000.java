package com.revature;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.revature.map.USFemEducationSince2000Mapper;
import com.revature.models.DoubleArrayWritable;
import com.revature.reduce.AverageReducer;
import com.revature.reduce.YearlyDifferenceReducer;

/**
 * Request: List the average increase in female education in the U.S. from the year 2000
 * 
 * This job is to list the average annual increase in female school enrollment for all three
 * education levels in the U.S. between the year 2000 and the last year of data available.
 *
 */

public class AvgIncreaseInUSFemEdFrom2000 extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job();
		job.setJarByClass(AvgIncreaseInUSFemEdFrom2000.class);

		job.setJobName("Average increase in U.S. female education from 2000");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));


		job.setMapperClass(USFemEducationSince2000Mapper.class);
		job.setCombinerClass(YearlyDifferenceReducer.class);
		job.setReducerClass(AverageReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		boolean success = job.waitForCompletion(true);
		return (success ? 0 : 1);
	}

}
