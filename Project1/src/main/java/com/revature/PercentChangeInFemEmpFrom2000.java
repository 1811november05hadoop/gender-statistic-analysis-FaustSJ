package com.revature;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.revature.map.GlobalFemEmploymentSince2000Mapper;
import com.revature.reduce.DifferenceReducer;

/*
 * 
 * 
 *  * List the % of change in male employment from the year 2000
 * List the % of change in female employment from the year 2000
 * Do you have a specific country in mind? list of countries?
 *	There is a World estimate if you would want that.
 * 
 * 28716 
 * 28718
 * 
 * final, 
 * combiner: dif in female vs male education percentages
 * reduce: country where difference decreases the most
 */
public class PercentChangeInFemEmpFrom2000 extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		//just copy all code from WordCount main()

		if(args.length != 2) {
			System.err.println("Usage: PercentChangeInFemEmpFrom2000 <input dir> <output dir>");
			return -1;
		}

		Job job = new Job();
		job.setJarByClass(PercentChangeInFemEmpFrom2000.class);

		job.setJobName("Percent Change in female employment from 2000");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));


		job.setMapperClass(GlobalFemEmploymentSince2000Mapper.class);
		job.setReducerClass(DifferenceReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean success = job.waitForCompletion(true);
		return (success ? 0 : 1);

	}
}
