package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.revature.models.DoubleArrayWritable;

/*
 * I could've used net or gross values, but I'm using gross values because
 * 		there are no net values for tertiary education enrollment
 * 
 * School enrollment, primary, female (% gross)
 * School enrollment, secondary, female (% gross)
 * School enrollment, tertiary, female (% gross)
 */

public class USFemEducationSince2000Mapper extends Mapper<LongWritable, Text, Text, DoubleArrayWritable>{

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
		if(stats[0].equals("United States")) {

			//set up an array of Doubles representing the years 2000 through 2016
			DoubleWritable[] data = new DoubleWritable[17];
			//to add years to the key text, it needs to be calculated in a separate block,
			//		so this boolean marks whether to enter block and calculate years before writing to output.
			boolean writeToContext = false;

			if(stats[2].equals("School enrollment, primary, female (% gross)")) {
				for(int i = 44; i<stats.length; i++) {
					try {
						data[i-44] = new DoubleWritable(Double.parseDouble(stats[i]));
					} catch (Exception e) {
						//don't report an error, just store -1 for non-numeric or absent values
						//	(-1 because the actual data is a percentage, never going below 0)
						data[i-44] = new DoubleWritable(-1.0);
					}
				}
				
				writeToContext = true;
			}
			else if(stats[2].equals("School enrollment, secondary, female (% gross)")) {

				for(int i = 44; i<stats.length; i++) {
					try {
						data[i-44] = new DoubleWritable(Double.parseDouble(stats[i]));
					} catch (Exception e) {
						//don't report an error, just store -1 for non-numeric or absent values
						//	(-1 because the actual data is a percentage, never going below 0)
						data[i-44] = new DoubleWritable(-1.0);
					}
				}
				writeToContext = true;
			}
			else if(stats[2].equals("School enrollment, tertiary, female (% gross)")) {

				for(int i = 44; i<stats.length; i++) {
					try {
						data[i-44] = new DoubleWritable(Double.parseDouble(stats[i]));
					} catch (Exception e) {
						//don't report an error, just store -1 for non-numeric or absent values
						//	(-1 because the actual data is a percentage, never going below 0)
						data[i-44] = new DoubleWritable(-1.0);
					}
				}
				
				writeToContext = true;
			}
			
			//This block calculates the years to list in the output, then writes the output.
			//It also makes sure there is more than one data value to work with.
			if(writeToContext) {
				int firstYear = 2000;
				int lastYear = 2016;
				//find first year
				for(int i=16; i>=0; i--) {
					if(data[i].get()!=-1.0) {
						break;
					}
					lastYear--;
				}
				//find last year
				for(int i=0; i<=16; i++) {
					if(data[i].get()!=-1.0) {
						break;
					}
					firstYear++;
				}
				//if the first year and the last year aren't the same year, write to output
				if(lastYear!=firstYear) {
					context.write(new Text(stats[0] + ", " + stats[2] + " ("+firstYear+" through "+lastYear+"): "), new DoubleArrayWritable(data));
				}
			}
		}
	}
}

