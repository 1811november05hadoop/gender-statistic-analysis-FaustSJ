package com.revature.reduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.revature.models.DoubleArrayWritable;

//Acts as a combiner, making and outputting a list of the differences between yearly values
public class YearlyDifferenceReducer extends Reducer<Text, DoubleArrayWritable, Text, DoubleArrayWritable>{
	
	//fields needed for testing purposes
	private Text outputKey = new Text();
	private DoubleArrayWritable outputValue = new DoubleArrayWritable();
	
	//reduce(input key, input values list, output)
	public void reduce(Text key, Iterable<DoubleArrayWritable> values, Context context)
			throws IOException, InterruptedException {
		
		//The mapper sends an array as a value, which is added to an iterable.
		for(DoubleArrayWritable doubleArray: values){
			DoubleWritable[] doubleWritables = doubleArray.get();
			
			//every year should have a value, except the last couple of years,
			//		so we don't need to worry about interpolating missing inner values.
			
			ArrayList<DoubleWritable> differences = new ArrayList<>();

			//We need to access index+1, so we'll cap at index length-2
			for(int i=0; i<(doubleWritables.length-2); i++) {
				double valOne = doubleWritables[i].get();
				double valTwo = doubleWritables[i+1].get();
				
				if(valTwo!=-1.0) {
					differences.add(new DoubleWritable((double)Math.round((valTwo-valOne) * 100000d) / 100000d));
				}
			}
			
			DoubleWritable[] diffArr = new DoubleWritable[differences.size()];
			for(int i = 0; i<differences.size(); i++){
				diffArr[i] = differences.get(i);
			}
			
			//convert the arraylist to an array of DoubleWritables and output it
			context.write(key, new DoubleArrayWritable(diffArr));
		}
	}
}
