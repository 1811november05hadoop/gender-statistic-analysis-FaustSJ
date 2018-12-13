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

/*
 *  * Identify the countries where % of female graduates is less than 30%
 * Do you have a particular year in mind? I could do 2016, but 2014 has the most
 * 	data available.
 * By "graduates" are you referring to the population in Masters and Doctorate programs?
 * 	Or are you referring to the population that graduates from tertiary schools.
 * 
 * South Asia (IDA & IBRD), School enrollment, tertiary, female (% gross) (2014): 	19.9473209381104
South Asia, School enrollment, tertiary, female (% gross) (2014): 	19.9473209381104
Sri Lanka, School enrollment, tertiary, female (% gross) (2015): 	23.95536
St. Lucia, School enrollment, tertiary, female (% gross) (2015): 	22.01136
St. Vincent and the Grenadines, School enrollment, tertiary, female (% gross) (1990): 	8.49879
Sub-Saharan Africa (IDA & IBRD), School enrollment, tertiary, female (% gross) (2014): 	7.242760181427
Sub-Saharan Africa (excluding high income), School enrollment, tertiary, female (% gross) (2014): 	7.24258995056152
Sub-Saharan Africa, School enrollment, tertiary, female (% gross) (2014): 	7.242760181427
Sudan, School enrollment, tertiary, female (% gross) (2014): 	16.82608

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
