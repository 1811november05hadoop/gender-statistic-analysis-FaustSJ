package com.revature;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.revature.map.TertiaryEducationGPIMapper;
import com.revature.models.DoubleArrayWritable;
import com.revature.reduce.GPIDifferenceReducer;

/**
 * 
 * This job looks at tertiary enrollment rates for men versus women using the Gender Parity Index.
 * A GPI of less than 1 means men outnumber women, greater than 1 means women outnumber men, 1 means their equal.
 * 
 * We'll be focusing on countries whose GPI have risen within the five most recent years between 2010 and 2016.
 * 		where women are outnumbered in tertiary education.
 * 
 * Finding countries whose GPI has risen within the the five most recent years might have "special programs
 *		aimed at women" that positively impact womens' involvement in higher education.
 *
 */

public class GenderParityIndexIncreasedInFiveYears extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		//just copy all code from WordCount main()

		if(args.length != 2) {
			System.err.println("Usage: GenderParityIndexIncreasedInFiveYears <input dir> <output dir>");
			return -1;
		}

		Job job = new Job();
		job.setJarByClass(PercentChangeInFemEmpFrom2000.class);

		job.setJobName("Countries whose GPI has risen in five most recent years");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));


		job.setMapperClass(TertiaryEducationGPIMapper.class);
		job.setReducerClass(GPIDifferenceReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		boolean success = job.waitForCompletion(true);
		return (success ? 0 : 1);

	}
}
