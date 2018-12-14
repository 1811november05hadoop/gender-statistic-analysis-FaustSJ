package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.revature.models.DoubleArrayWritable;

public class TertiaryEducationGPIMapper extends Mapper<LongWritable, Text, Text, DoubleArrayWritable>{

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
		 *  14 - 1970
		 *  24 - 1980
		 *  34 - 1990
		 *  44 - 2000
		 *  54 - 2010
		 *  60 - 2016 (if length = 62)
		 */

		//split the lines through " " spaces
		String[] stats = line.split("\",\"");
		stats[0] = stats[0].replace("\"", "");
		stats[stats.length-1] = stats[stats.length-1].replace("\"", "");
		stats[stats.length-1] = stats[stats.length-1].replace(",", "");
		if(stats[2].equals("School enrollment, tertiary (gross), gender parity index (GPI)")) {

			//set up an array of Doubles holding the data for the latest year and the year 5-years-prior
			double[] dataDouble = {-1, -1};
			
			//Get the most recent year data available, starting at 2016(index=60).
			//If nothing's available before reaching 2009(index=53), 
			//		skip this country (we want relatively recent data). 
			int latestYear = 2016;
			for(int i = 60; i>53; i--) {
				try {
					dataDouble[1] = Double.parseDouble(stats[i]);
					break;
				} catch (Exception e) {
					//don't report an error, just check the next year prior
				}
				latestYear--;
			}
			
			int furthestYear = 2010;
			for(int i=54; i<=60; i++) {
				try {
					dataDouble[0] = Double.parseDouble(stats[i]);
					break;
				} catch (Exception e) {
					//don't report an error, just check the next year
				}
				furthestYear++;
			}
			
			//We only want GPI values that start less than 1 (meaning men outnumber women), because we want to find
			//		countries where women are at a disadvantage, but conditions are improving. 
			if((dataDouble[0]>=1.0) || (dataDouble[0]>=dataDouble[1])){
				return;
			}
			
			DoubleWritable[] dataDoubleWriter = new DoubleWritable[2];
			dataDoubleWriter[0] = new DoubleWritable(dataDouble[0]);
			dataDoubleWriter[1] = new DoubleWritable(dataDouble[1]);
			context.write(new Text(stats[0] + ", tertiary school enrollment\nGPI increase from "+furthestYear+" to "+latestYear+": "), new DoubleArrayWritable(dataDoubleWriter));
		}
	}
}