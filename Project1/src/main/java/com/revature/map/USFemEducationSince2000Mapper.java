package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/*
 * Educational attainment, completed primary, population 25+ years, female (%)
 * Educational attainment, completed lower secondary, population 25+ years, female (%)
 * Educational attainment, completed upper secondary, population 25+ years, female (%)
 * Educational attainment, completed post-secondary, population 25+ years, female (%)
 */

public class USFemEducationSince2000Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
	// TODO Auto-generated method stub

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
		if(stats[0].equals("United States")) {
			if(stats[2].equals("Educational attainment, completed primary, population 25+ years, female (%)")) {
				context.write(new Text(stats[0] + ", " + stats[2] + " "), 
						new DoubleWritable(Double.parseDouble(stats[60])));
			}
			else if(stats[2].equals("Educational attainment, completed lower secondary, population 25+ years, female (%)")) {
				
			}
			else if(stats[2].equals("Educational attainment, completed upper secondary, population 25+ years, female (%)")) {
				
			}
			else if(stats[2].equals("Educational attainment, completed post-secondary, population 25+ years, female (%)")) {
				
			}
		}
		if(stats[2].equals("School enrollment, tertiary, female (% gross)")) {
			
			//many countries don't have data for every year, so we'll gather whatever data is available in the
			//		three most recent years (2016, 2015, and 2014)
			
			if(!stats[60].equals("\",")) {
				context.write(new Text(stats[0] + ", " + stats[2] + " (2016)"), 
						new DoubleWritable(Double.parseDouble(stats[60])));
			}
			else if(stats[59].length()>0) {
				context.write(new Text(stats[0] + ", " + stats[2] + " (2015)"), 
						new DoubleWritable(Double.parseDouble(stats[60])));
			}
			else if(stats[58].length()>0) {
				context.write(new Text(stats[0] + ", " + stats[2] + " (2014)"), 
						new DoubleWritable(Double.parseDouble(stats[60])));
			}
		}
	}
}

