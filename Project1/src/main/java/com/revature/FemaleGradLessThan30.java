package com.revature;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.revature.map.FemaleGraduatesMapper;
import com.revature.reduce.LessThan30Reducer;

/**
 * Request: Identify the countries where % of female graduates is less than 30%
 * 
 * This job is to list countries (or regions) where females in tertiary education
 * is less than 30%
 *
 */

public class FemaleGradLessThan30 extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		//just copy all code from WordCount main()

		if(args.length != 2) {
			System.err.println("Usage: FemaleGradLessThan30 <input dir> <output dir>");
			return -1;
		}

		Job job = new Job();
		job.setJarByClass(FemaleGradLessThan30.class);

		job.setJobName("Contries where female graduates are less than 30%");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));


		job.setMapperClass(FemaleGraduatesMapper.class);
		job.setReducerClass(LessThan30Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		boolean success = job.waitForCompletion(true);
		return (success ? 0 : 1);

	}
}
