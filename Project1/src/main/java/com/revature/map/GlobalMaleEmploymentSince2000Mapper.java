package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.revature.models.DoubleArrayWritable;

public class GlobalMaleEmploymentSince2000Mapper extends Mapper<LongWritable, Text, Text, DoubleArrayWritable>{

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
		if(stats[2].equals("Employment to population ratio, 15+, male (%) (modeled ILO estimate)")) {

			//set up an array of Doubles holding the data for 2000 and the last year of available data
			double[] dataDouble = {-1, -1};
			
			try {
				//store the data for the year 2000
				dataDouble[0] = Double.parseDouble(stats[44]);
			} catch (Exception e ) {
				//if there is no data for the year 2000, don't write it as output
				return;
			}
			
			//44 is the index of 2000, +16 for 2016
			int latestYear = 2016;
			for(int i = (44+16); i>44; i--) {
				try {
					dataDouble[1] = Double.parseDouble(stats[i]);
					break;
				} catch (Exception e) {
					//don't report an error, just go to the next loop step
				}
				latestYear--;
			}
			
			//if two values were found, write them into a DoubleArrayWritable...
			if((dataDouble[0]!=-1)&&(dataDouble[1]!=-1)) {
				DoubleWritable[] dataDoubleWriter = new DoubleWritable[2];
				dataDoubleWriter[0] = new DoubleWritable(dataDouble[0]);
				dataDoubleWriter[1] = new DoubleWritable(dataDouble[1]);
				context.write(new Text(stats[0] + ", change in the % of males employed,\n\t2000 compared to "+latestYear+": "), new DoubleArrayWritable(dataDoubleWriter));
			}
			//...else, just don't output it to the reducer
		}
	}
}
