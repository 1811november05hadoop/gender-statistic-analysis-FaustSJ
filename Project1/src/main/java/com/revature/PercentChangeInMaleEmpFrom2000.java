package com.revature;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.revature.map.GlobalMaleEmploymentSince2000Mapper;
import com.revature.models.DoubleArrayWritable;
import com.revature.reduce.DifferenceReducer;

/**
 * Request: List the % of change in male employment from the year 2000
 * 
 * This job is to list how the % of males employed changed from 2000
 * 		to the most recent year available, for every country (or region)
 *
 */

public class PercentChangeInMaleEmpFrom2000 extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {

		Job job = new Job();
		job.setJarByClass(PercentChangeInMaleEmpFrom2000.class);

		job.setJobName("Percent Change in male employment from 2000");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));


		job.setMapperClass(GlobalMaleEmploymentSince2000Mapper.class);
		job.setReducerClass(DifferenceReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		return (success ? 0 : 1);

	}
}
