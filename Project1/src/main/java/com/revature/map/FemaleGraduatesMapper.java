package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FemaleGraduatesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{
		//Hadoop throws these exceptions, we duck them

		//get a line from the flatfile
		String line = value.toString();

		/*
		 * indexs:
		 *  0 - country
		 *  2 - statistic
		 *  4 - 1960
		 *  60 - 2016 (if length = 62)
		 */

		//split the lines through " " spaces
		String[] stats = line.split("\",\"");

		if(stats[2].equals("School enrollment, tertiary, female (% gross)")) {
			
			//many countries don't have data for every year, so we'll gather whatever data is available in the
			//		three most recent years (2016, 2015, and 2014)
			
			int latestYear = 2016;
			double data = 0.0;
			
			for(int i = 60; i>3; i--) {
				try {
					data = Double.parseDouble(stats[i]);
				} catch (Exception e) {
					//don't print any errors, just move on to the the next loop step
				}
				latestYear--;
			}
			
			//if data was found, write it to the output...
			if(data!=0.0) {
				context.write(new Text(stats[0] + ", " + stats[2] + " ("+latestYear+"): "), 
						new DoubleWritable(Double.parseDouble(stats[60])));
			}
			//...else, don't add anything to the output
		}
	}
}
