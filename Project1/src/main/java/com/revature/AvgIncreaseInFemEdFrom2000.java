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
import com.revature.reduce.AverageReducer;
import com.revature.reduce.YearlyDifferenceReducer;

/*
 * List the average (yearly) increase in female education in the U.S. from the year 2000
 */
public class AvgIncreaseInFemEdFrom2000 extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//just copy all code from WordCount main()

		if(args.length != 2) {
			System.err.println("Usage: AvgIncreaseInFemEdFrom2000 <input dir> <output dir>");
			return -1;
		}

		Job job = new Job();
		job.setJarByClass(AvgIncreaseInFemEdFrom2000.class);

		job.setJobName("Average increase in U.S. female education from 2000");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));


		job.setMapperClass(USFemEducationSince2000Mapper.class);
		job.setCombinerClass(YearlyDifferenceReducer.class);
		job.setReducerClass(AverageReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		boolean success = job.waitForCompletion(true);
		return (success ? 0 : 1);
	}

}
