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
		stats[0] = stats[0].replace("\"", "");
		stats[stats.length-1] = stats[stats.length-1].replace("\"", "");
		stats[stats.length-1] = stats[stats.length-1].replace(",", "");
		if(stats[2].equals("School enrollment, tertiary, female (% gross)")) {
			
			//many countries don't have data for every year, so we'll gather whatever data is available in the
			//		most recent year
			
			int latestYear = 2016;
			double data = 0.0;
			boolean dataFound = false;
			
			for(int i = 60; i>3; i--) {
				try {
					data = Double.parseDouble(stats[i]);
					dataFound = true;
					break;
				} catch (Exception e) {
					//don't print any errors, just move on to the the next loop step
				}
				latestYear--;
			}
			
			//if data was found, write it to the output...
			if(dataFound) {
				context.write(new Text(stats[0] + "\n\t female tertiary school enrollment for "+latestYear+" (% gross): "), 
						new DoubleWritable(data));
			}
			//...else, don't add anything to the output
		}
	}
}
